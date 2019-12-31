using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Node.Communication.Clients;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Raft
{
    public class RaftService<State> : BaseService<State>, IRaftService where State : BaseState, new()
    {
        private Timer _heartbeatTimer;
        private Timer _electionTimeoutTimer;
        private Task _commitTask;
        private Task _bootstrapTask;
        private Task _nodeDiscoveryTask;
        private Bootstrapper<State> _bootstrapService;
        private Discovery _discovery;
        private CommitService<State> _commitService;


        private IClusterConnectionPool _clusterConnectionPool;
        //Used to track whether you are currently already sending logs to a particular node to not double send
        public ConcurrentDictionary<Guid, bool> LogsSent = new ConcurrentDictionary<Guid, bool>();
        public ClusterClient _clusterClient;

        private INodeStorage<State> _nodeStorage;
        Snapshotter<State> _snapshotService;
        object VoteLock = new object();
        ILoggerFactory _loggerFactory { get; set; }

        public RaftService(
            ILoggerFactory logger,
            IOptions<ClusterOptions> clusterOptions,
            IOptions<NodeOptions> nodeOptions,
            IClusterConnectionPool clusterConnectionPool,
            INodeStorage<State> nodeStorage,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient
            ) : base(logger.CreateLogger<RaftService<State>>(), clusterOptions.Value, nodeOptions.Value, stateMachine, nodeStateService)
        {
            _nodeStorage = nodeStorage;
            _loggerFactory = logger;
            //Bootstrap the node
            _snapshotService = new Snapshotter<State>(logger.CreateLogger<Snapshotter<State>>(), nodeStorage, stateMachine, nodeStateService);

            _bootstrapService = new Bootstrapper<State>(logger.CreateLogger<Bootstrapper<State>>(), clusterOptions.Value, nodeOptions.Value, nodeStorage, StateMachine, NodeStateService);
            _commitService = new CommitService<State>(logger.CreateLogger<CommitService<State>>(), clusterOptions.Value, nodeOptions.Value, nodeStorage, StateMachine, NodeStateService);
            _discovery = new Discovery(logger.CreateLogger<Discovery>());
            _clusterClient = clusterClient;
            _clusterConnectionPool = clusterConnectionPool;
            NodeStateService.Id = _nodeStorage.Id;

            if (!ClusterOptions.TestMode)
            {
                _bootstrapTask = Task.Run(() =>
                {
                    //Wait for the rest of the node to bootup
                    Thread.Sleep(3000);
                    nodeStateService.Url = _bootstrapService.GetMyUrl(ClusterOptions.GetClusterUrls(), TimeSpan.FromMilliseconds(ClusterOptions.LatencyToleranceMs)).GetAwaiter().GetResult();
                    NodeStateService.IsBootstrapped = true;
                });
            }

            _electionTimeoutTimer = new Timer(ElectionTimeoutEventHandler);
            _heartbeatTimer = new Timer(HeartbeatTimeoutEventHandler);

            if (ClusterOptions.TestMode)
            {
                Logger.LogInformation("Running in test mode...");
                SetNodeRole(NodeState.Leader);
            }
            else
            {
                SetNodeRole(NodeState.Follower);
            }
        }

        public void HeartbeatTimeoutEventHandler(object args)
        {
            if (NodeStateService.IsBootstrapped)
            {
                Logger.LogDebug("Detected heartbeat timeout event.");
                SendHeartbeats();
            }
        }

        public async Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {
            DateTime commandStartTime = DateTime.Now;
            TResponse response;
            switch (request)
            {
                case ExecuteCommands t1:
                    response = (TResponse)(object)ExecuteCommandsRPCHandler(t1);
                    break;
                case RequestVote t1:
                    response = (TResponse)(object)RequestVoteRPCHandler(t1);
                    break;
                case AppendEntry t1:
                    response = (TResponse)(object)AppendEntryRPCHandler(t1);
                    break;
                case InstallSnapshot t1:
                    response = (TResponse)(object)InstallSnapshotHandler(t1);
                    break;
                default:
                    throw new Exception("Request is not implemented");
            }

            return response != null ? response : new TResponse()
            {
                IsSuccessful = false,
                ErrorMessage = "Response is null"
            };
        }

        public InstallSnapshotResponse InstallSnapshotHandler(InstallSnapshot request)
        {
            if (request.Term < _nodeStorage.CurrentTerm)
            {
                return new InstallSnapshotResponse()
                {
                    IsSuccessful = false,
                    Term = request.Term
                };
            }
            return new InstallSnapshotResponse()
            {
                IsSuccessful = _snapshotService.InstallSnapshot(request.Snapshot, request.LastIncludedIndex, request.LastIncludedTerm),
                Term = request.Term
            };
        }

        public ExecuteCommandsResponse ExecuteCommandsRPCHandler(ExecuteCommands request)
        {
            int index = _nodeStorage.AddCommands(request.Commands.ToList(), _nodeStorage.CurrentTerm);
            var startDate = DateTime.Now;
            while (request.WaitForCommits)
            {
                if ((DateTime.Now - startDate).TotalMilliseconds > ClusterOptions.CommitsTimeout)
                {
                    return new ExecuteCommandsResponse()
                    {
                        EntryNo = index,
                        IsSuccessful = false
                    };
                }

                Logger.LogDebug(NodeStateService.GetNodeLogId() + "Waiting for " + request.RequestName + " to complete.");
                if (NodeStateService.CommitIndex >= index)
                {
                    return new ExecuteCommandsResponse()
                    {
                        EntryNo = index,
                        IsSuccessful = true
                    };
                }
                else
                {
                    Thread.Sleep(100);
                }
            }
            return new ExecuteCommandsResponse()
            {
                EntryNo = index,
                IsSuccessful = true
            };
        }

        public RequestVoteResponse RequestVoteRPCHandler(RequestVote requestVoteRPC)
        {
            var successful = false;
            if (NodeStateService.IsBootstrapped)
            {
                //To requests might come in at the same time causing the VotedFor to not match
                lock (VoteLock)
                {
                    //Ref1 $5.2, $5.4
                    if (_nodeStorage.CurrentTerm <= requestVoteRPC.Term && ((_nodeStorage.VotedFor == null || _nodeStorage.VotedFor == requestVoteRPC.CandidateId) &&
                    (requestVoteRPC.LastLogIndex >= _nodeStorage.GetTotalLogCount() && requestVoteRPC.LastLogTerm >= _nodeStorage.GetLastLogTerm())))
                    {
                        _nodeStorage.SetVotedFor(requestVoteRPC.CandidateId);
                        Logger.LogDebug(NodeStateService.GetNodeLogId() + "Voting for " + requestVoteRPC.CandidateId + " for term " + requestVoteRPC.Term);
                        SetNodeRole(NodeState.Follower);
                        _nodeStorage.SetCurrentTerm(requestVoteRPC.Term);
                        successful = true;
                    }
                    else if (_nodeStorage.CurrentTerm > requestVoteRPC.Term)
                    {
                        Logger.LogDebug(NodeStateService.GetNodeLogId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as current term is greater (" + requestVoteRPC.Term + "<" + _nodeStorage.CurrentTerm + ")");
                    }
                    else if (requestVoteRPC.LastLogIndex < _nodeStorage.GetTotalLogCount() - 1)
                    {
                        Logger.LogDebug(NodeStateService.GetNodeLogId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as last log index is less then local index (" + requestVoteRPC.LastLogIndex + "<" + (_nodeStorage.GetTotalLogCount() - 1) + ")");
                    }
                    else if (requestVoteRPC.LastLogTerm < _nodeStorage.GetLastLogTerm())
                    {
                        Logger.LogDebug(NodeStateService.GetNodeLogId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as last log term is less then local term (" + requestVoteRPC.LastLogTerm + "<" + _nodeStorage.GetLastLogTerm() + ")");
                    }
                    else if ((_nodeStorage.VotedFor != null && _nodeStorage.VotedFor != requestVoteRPC.CandidateId))
                    {
                        Logger.LogDebug(NodeStateService.GetNodeLogId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as I have already voted for " + _nodeStorage.VotedFor + " | ");
                    }
                    else if (!successful)
                    {
                        Logger.LogError("Rejected vote from " + requestVoteRPC.CandidateId + " due to unknown reason.");
                    }
                }
            }
            return new RequestVoteResponse()
            {
                NodeId = _nodeStorage.Id,
                IsSuccessful = successful
            };
        }

        public AppendEntryResponse AppendEntryRPCHandler(AppendEntry entry)
        {
            //Check the log check to prevent a intermittent term increase with no back tracking, TODO check whether this causes potentially concurrency issues
            if (entry.Term < _nodeStorage.CurrentTerm && entry.LeaderCommit <= NodeStateService.CommitIndex)
            {
                Logger.LogDebug(NodeStateService.GetNodeLogId() + "Rejected RPC from " + entry.LeaderId + " due to lower term " + entry.Term + "<" + _nodeStorage.CurrentTerm);
                return new AppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.OldTermException,
                    NodeId = _nodeStorage.Id,
                    IsSuccessful = false
                };
            }

            //Reset the timer if the append is from a valid term
            ResetTimer(_electionTimeoutTimer, ClusterOptions.ElectionTimeoutMs, ClusterOptions.ElectionTimeoutMs);

            //If you are a leader or candidate, swap to a follower
            if (NodeStateService.Role == NodeState.Candidate || NodeStateService.Role == NodeState.Leader)
            {
                Logger.LogDebug(NodeStateService.GetNodeLogId() + " detected node " + entry.LeaderId + " is further ahead. Changing to follower");
                SetNodeRole(NodeState.Follower);
            }

            if (NodeStateService.CurrentLeader != entry.LeaderId)
            {
                Logger.LogDebug(NodeStateService.GetNodeLogId() + "Detected uncontacted leader, discovering leader now.");
                //Reset the current leader
                NodeStateService.CurrentLeader = entry.LeaderId;
            }

            if (entry.LeaderCommit > NodeStateService.LatestLeaderCommit)
            {
                Logger.LogDebug(NodeStateService.GetNodeLogId() + "Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                NodeStateService.LatestLeaderCommit = entry.LeaderCommit;
            }

            // If your log entry is not within the last snapshot, then check the validity of the previous log index
            if (_nodeStorage.LastSnapshotIncludedIndex < entry.PrevLogIndex)
            {
                var previousEntry = _nodeStorage.GetLogAtIndex(entry.PrevLogIndex);

                if (previousEntry == null && entry.PrevLogIndex != 0)
                {
                    Logger.LogDebug(NodeStateService.GetNodeLogId() + "Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");

                    return new AppendEntryResponse()
                    {
                        IsSuccessful = false,
                        ConflictingTerm = null,
                        ConflictName = AppendEntriesExceptionNames.MissingLogEntryException,
                        FirstTermIndex = null,
                        NodeId = _nodeStorage.Id,
                        LastLogEntryIndex = _nodeStorage.GetTotalLogCount()
                    };
                }
            }
            else
            {
                if (_nodeStorage.LastSnapshotIncludedTerm != entry.PrevLogTerm)
                {
                    Logger.LogDebug(NodeStateService.GetNodeLogId() + "Inconsistency found in the node snapshot and leaders logs, log " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");
                    return new AppendEntryResponse()
                    {
                        ConflictName = AppendEntriesExceptionNames.ConflictingLogEntryException,
                        IsSuccessful = false,
                        ConflictingTerm = entry.PrevLogTerm,
                        NodeId = _nodeStorage.Id,
                        FirstTermIndex = 0 //always set to 0 as snapshots are assumed to be from 0 > n
                    };
                }
            }

            _nodeStorage.SetCurrentTerm(entry.Term);

            foreach (var log in entry.Entries.OrderBy(e => e.Index))
            {
                var existingEnty = _nodeStorage.GetLogAtIndex(log.Index);
                if (existingEnty != null && existingEnty.Term != log.Term)
                {
                    Logger.LogDebug(NodeStateService.GetNodeLogId() + "Found inconsistent logs in state, deleting logs from index " + log.Index);
                    _nodeStorage.DeleteLogsFromIndex(log.Index);
                    break;
                }
            }

            NodeStateService.InCluster = true;

            DateTime time = DateTime.Now;

            foreach (var log in entry.Entries)
            {
                try
                {
                    _nodeStorage.AddLog(log);
                }
                catch (MissingLogEntryException e)
                {
                    return new AppendEntryResponse()
                    {
                        IsSuccessful = false,
                        ConflictingTerm = null,
                        ConflictName = AppendEntriesExceptionNames.MissingLogEntryException,
                        FirstTermIndex = null,
                        NodeId = _nodeStorage.Id,
                        LastLogEntryIndex = _nodeStorage.GetTotalLogCount()
                    };
                }
                catch (Exception e)
                {
                    Logger.LogError(NodeStateService.GetNodeLogId() + " failed to add log " + log.Index + " with exception " + e.Message + Environment.NewLine + e.StackTrace);
                    return new AppendEntryResponse()
                    {
                        IsSuccessful = false,
                        ConflictingTerm = null,
                        ConflictName = AppendEntriesExceptionNames.LogAppendingException,
                        FirstTermIndex = null,
                        NodeId = _nodeStorage.Id,
                        LastLogEntryIndex = _nodeStorage.GetTotalLogCount()
                    };
                }
            }

            return new AppendEntryResponse()
            {
                IsSuccessful = true,
                NodeId = _nodeStorage.Id,
            };
        }

        private void ResetTimer(Timer timer, int dueTime, int period)
        {
            timer.Change(dueTime, period);
        }

        private void StopTimer(Timer timer)
        {
            ResetTimer(timer, Timeout.Infinite, Timeout.Infinite);
        }

        public void SetNodeRole(NodeState newState)
        {
            if (newState == NodeState.Candidate && !NodeOptions.EnableLeader)
            {
                Logger.LogWarning(NodeStateService.GetNodeLogId() + "Tried to promote to candidate but node has been disabled.");
                return;
            }

            if (newState != NodeStateService.Role)
            {
                Logger.LogInformation(NodeStateService.GetNodeLogId() + "Node's role changed to " + newState.ToString());
                NodeStateService.SetRole(newState);

                switch (newState)
                {
                    case NodeState.Candidate:
                        ResetTimer(_electionTimeoutTimer, ClusterOptions.ElectionTimeoutMs, ClusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        NodeStateService.InCluster = false;
                        StartElection();
                        break;
                    case NodeState.Follower:
                        Random rand = new Random();
                        //On becoming a follower, wait 5 seconds to allow any other nodes to send out election time outs
                        ResetTimer(_electionTimeoutTimer, rand.Next(ClusterOptions.ElectionTimeoutMs, ClusterOptions.ElectionTimeoutMs * 2), ClusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        TaskUtility.RestartTask(ref _commitTask, () => MonitorCommits());
                        break;
                    case NodeState.Leader:
                        NodeStateService.CurrentLeader = _nodeStorage.Id;
                        NodeStateService.ResetLeaderState();
                        ResetTimer(_heartbeatTimer, 0, ClusterOptions.ElectionTimeoutMs / 4);
                        StopTimer(_electionTimeoutTimer);
                        TaskUtility.RestartTask(ref _commitTask, () => MonitorCommits());
                        NodeStateService.InCluster = true;
                        TaskUtility.RestartTask(ref _nodeDiscoveryTask, () => NodeDiscoveryLoop());
                        _clusterConnectionPool.CheckClusterConnectionPool();
                        break;
                    case NodeState.Disabled:
                        StopTimer(_electionTimeoutTimer);
                        StopTimer(_heartbeatTimer);
                        break;
                }
            }
        }

        public async Task NodeDiscoveryLoop()
        {
            while (NodeStateService.Role == NodeState.Leader)
            {
                try
                {
                    var urls = ClusterOptions.GetClusterUrls().Where(cl => !StateMachine.IsNodeContactable(cl));
                    if (urls.Count() > 0)
                    {
                        Logger.LogInformation("Starting discovery...");
                        var updates = await _discovery.SearchForNodes(urls, TimeSpan.FromMilliseconds(ClusterOptions.LatencyToleranceMs));
                        if (updates.Count() > 0)
                        {
                            Logger.LogInformation("Found a node");
                            AddNodesToCluster(updates);
                            var myNodeInfo = updates.Where(u => u.Id == _nodeStorage.Id);
                            if (myNodeInfo.Count() > 0)
                            {
                                NodeStateService.Url = myNodeInfo.First().TransportAddress;
                            }
                        }
                        Thread.Sleep(100);
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to run Cluster Info Timeout Handler with error " + e.StackTrace);
                }
            }
        }

        public async void ElectionTimeoutEventHandler(object args)
        {
            SetNodeRole(NodeState.Candidate);
        }

        public async void StartElection()
        {
            try
            {
                var lastLogTerm = _nodeStorage.GetLastLogTerm();
                if (_nodeStorage.CurrentTerm > lastLogTerm + 3)
                {
                    Logger.LogInformation("Detected that the node is too far ahead of its last log (3), restarting election from the term of the last log term " + lastLogTerm);
                    _nodeStorage.SetCurrentTerm(lastLogTerm);
                }
                else
                {
                    _nodeStorage.SetCurrentTerm(_nodeStorage.CurrentTerm + 1);
                }
                //Vote for yourself
                _nodeStorage.SetVotedFor(_nodeStorage.Id);
                var election = new Election(_loggerFactory.CreateLogger<Election>(), TimeSpan.FromMilliseconds(ClusterOptions.LatencyToleranceMs), ClusterOptions.GetClusterUrls().Where(url => url != NodeStateService.Url));
                var collectedNodes = await election.CollectVotes(
                    _nodeStorage.CurrentTerm,
                    _nodeStorage.Id,
                    _nodeStorage.GetLastLogIndex(),
                    _nodeStorage.GetLastLogTerm());
                if (collectedNodes.Count() >= ClusterOptions.MinimumNodes)
                {
                    Logger.LogInformation(NodeStateService.GetNodeLogId() + "Recieved enough votes to be promoted, promoting to leader. Registered nodes: " + collectedNodes.Count());
                    AddNodesToCluster(collectedNodes.Select(cn => new NodeInformation()
                    {
                        Id = cn.Key,
                        TransportAddress = cn.Value,
                        IsContactable = true,
                        Name = ""
                    }));
                    SetNodeRole(NodeState.Leader);
                }
                else
                {
                    NodeStateService.CurrentLeader = null;
                    _nodeStorage.SetVotedFor(null);
                    SetNodeRole(NodeState.Follower);
                }
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to run election with error " + e.StackTrace);
            }
        }

        public async Task MonitorCommits()
        {
            while (true)
            {
                try
                {
                    _commitService.ApplyCommits();
                    if (NodeStateService.Role == NodeState.Leader || NodeStateService.Role == NodeState.Follower)
                    {
                        _snapshotService.CheckAndApplySnapshots(NodeStateService.CommitIndex, ClusterOptions.SnapshottingTrailingLogCount, ClusterOptions.SnapshottingInterval);
                    }
                    Thread.Sleep(500);
                }
                catch (Exception e)
                {
                    Logger.LogWarning(NodeStateService.GetNodeLogId() + " encountered error " + e.Message + " with stacktrace " + e.StackTrace);
                }

            }
        }

        public async void SendHeartbeats()
        {
            if (NodeStateService.Role != NodeState.Leader)
            {
                Logger.LogWarning("Detected request to send hearbeat as non-leader");
                return;
            }

            var startTime = DateTime.Now;
            var recognizedhosts = 1;

            ConcurrentBag<Guid> unreachableNodes = new ConcurrentBag<Guid>();
            var clients = _clusterConnectionPool.GetAllNodeClients();
            if (clients.Count() < StateMachine.GetNodes().Count())
            {
                _clusterConnectionPool.CheckClusterConnectionPool();
                clients = _clusterConnectionPool.GetAllNodeClients();
            }

            var tasks = clients.Where(nc => nc.Key != _nodeStorage.Id && nc.Value.Address != NodeStateService.Url).Select(async node =>
            {
                try
                {
                    //Add the match index if required
                    if (!NodeStateService.NextIndex.ContainsKey(node.Key))
                        NodeStateService.NextIndex.Add(node.Key, _nodeStorage.GetTotalLogCount() + 1);
                    if (!NodeStateService.MatchIndex.ContainsKey(node.Key))
                        NodeStateService.MatchIndex.TryAdd(node.Key, 0);

                    Logger.LogDebug(NodeStateService.GetNodeLogId() + "Sending heartbeat to " + node.Key);
                    var entriesToSend = new List<LogEntry>();

                    var prevLogIndex = Math.Max(0, NodeStateService.NextIndex[node.Key] - 1);
                    var startingLogsToSend = NodeStateService.NextIndex[node.Key] == 0 ? 1 : NodeStateService.NextIndex[node.Key];
                    var unsentLogs = (_nodeStorage.GetTotalLogCount() - NodeStateService.NextIndex[node.Key] + 1);
                    var quantityToSend = unsentLogs;
                    var endingLogsToSend = startingLogsToSend + (quantityToSend < ClusterOptions.MaxLogsToSend ? quantityToSend : ClusterOptions.MaxLogsToSend) - 1;

                    //If the logs still exist in the state then get the logs to be sent
                    if (_nodeStorage.LastSnapshotIncludedIndex < startingLogsToSend)
                    {
                        int prevLogTerm = 0;
                        if (_nodeStorage.LastSnapshotIncludedIndex == prevLogIndex)
                        {
                            prevLogTerm = _nodeStorage.LastSnapshotIncludedTerm;
                        }
                        else
                        {
                            prevLogTerm = (_nodeStorage.GetTotalLogCount() > 0 && prevLogIndex > 0) ? _nodeStorage.GetLogAtIndex(prevLogIndex).Term : 0;
                        }
                        if (NodeStateService.NextIndex[node.Key] <= _nodeStorage.GetLastLogIndex() && _nodeStorage.GetLastLogIndex() != 0 && !LogsSent.GetOrAdd(node.Key, false))
                        {
                            entriesToSend = _nodeStorage.GetLogRange(startingLogsToSend, endingLogsToSend).ToList();
                            // entriesToSend = _nodeStorage.Logs.Where(l => l.Index >= NextIndex[connector.Key]).ToList();
                            Logger.LogDebug(NodeStateService.GetNodeLogId() + "Detected node " + node.Key + " is not upto date, sending logs from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index);

                        }

                        // Console.WriteLine("Sending logs with from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index + " sent " + entriesToSend.Count + "logs.");
                        LogsSent.AddOrUpdate(node.Key, true, (key, oldvalue) =>
                        {
                            return true;
                        });
                        var result = await _clusterClient.Send(node.Key, new AppendEntry()
                        {
                            Term = _nodeStorage.CurrentTerm,
                            Entries = entriesToSend,
                            LeaderCommit = NodeStateService.CommitIndex,
                            LeaderId = _nodeStorage.Id,
                            PrevLogIndex = prevLogIndex,
                            PrevLogTerm = prevLogTerm
                        });



                        LogsSent.TryUpdate(node.Key, false, true);

                        if (result != null && node.Key != result.NodeId && result.NodeId != default(Guid))
                        {
                            Logger.LogInformation("Detected change of client");
                            RemoveNodesFromCluster(new Guid[] { node.Key });
                            AddNodesToCluster(new NodeInformation[] { new NodeInformation {
                                Id = result.NodeId,
                                TransportAddress = node.Value.Address,
                                IsContactable = true,
                                Name = ""
                            } });
                        }

                        if (result == null)
                        {
                            Logger.LogError(NodeStateService.GetNodeLogId() + "The node " + node.Key + " has returned a empty response");
                        }
                        else if (result != null && result.IsSuccessful)
                        {
                            Logger.LogDebug(NodeStateService.GetNodeLogId() + "Successfully updated logs on " + node.Key);
                            if (entriesToSend.Count() > 0)
                            {
                                var lastIndexToSend = entriesToSend.Last().Index;
                                NodeStateService.NextIndex[node.Key] = lastIndexToSend + 1;

                                int previousValue;
                                bool SuccessfullyGotValue = NodeStateService.MatchIndex.TryGetValue(node.Key, out previousValue);
                                if (!SuccessfullyGotValue)
                                {
                                    Logger.LogError("Concurrency issues encountered when getting the Next Match Index");
                                }
                                var updateWorked = NodeStateService.MatchIndex.TryUpdate(node.Key, lastIndexToSend, previousValue);
                                //If the updated did not execute, there hs been a concurrency issue
                                while (!updateWorked)
                                {
                                    SuccessfullyGotValue = NodeStateService.MatchIndex.TryGetValue(node.Key, out previousValue);
                                    // If the match index has already exceeded the previous value, dont bother updating it
                                    if (previousValue > lastIndexToSend && SuccessfullyGotValue)
                                    {
                                        updateWorked = true;
                                    }
                                    else
                                    {
                                        updateWorked = NodeStateService.MatchIndex.TryUpdate(node.Key, lastIndexToSend, previousValue);
                                    }
                                }
                                Logger.LogDebug(NodeStateService.GetNodeLogId() + "Updated match index to " + NodeStateService.MatchIndex);
                            }
                        }
                        else if (result.ConflictName == AppendEntriesExceptionNames.MissingLogEntryException)
                        {
                            Logger.LogDebug(NodeStateService.GetNodeLogId() + "Detected node " + node.Key + " is missing the previous log, sending logs from log " + (result.LastLogEntryIndex.Value + 1));
                            NodeStateService.NextIndex[node.Key] = (result.LastLogEntryIndex.Value + 1);
                        }
                        else if (result.ConflictName == AppendEntriesExceptionNames.ConflictingLogEntryException)
                        {
                            var firstEntryOfTerm = _nodeStorage.Logs.Where(l => l.Value.Term == result.ConflictingTerm).FirstOrDefault();
                            var revertedIndex = firstEntryOfTerm.Value.Index < result.FirstTermIndex ? firstEntryOfTerm.Value.Index : result.FirstTermIndex.Value;
                            Logger.LogDebug(NodeStateService.GetNodeLogId() + "Detected node " + node.Key + " has conflicting values, reverting to " + revertedIndex);

                            //Revert back to the first index of that term
                            NodeStateService.NextIndex[node.Key] = revertedIndex;
                        }
                        else if (result.ConflictName == AppendEntriesExceptionNames.OldTermException)
                        {
                            Logger.LogDebug(NodeStateService.GetNodeLogId() + "Detected node " + node.Key + " rejected heartbeat due to invalid leader term.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation("Detected pending snapshot to send, sending snapshot with included index " + _nodeStorage.LastSnapshotIncludedIndex);

                        //Mark that you have send the logs to this node
                        LogsSent.AddOrUpdate(node.Key, true, (key, oldvalue) =>
                        {
                            return true;
                        });
                        var lastIndex = _nodeStorage.LastSnapshotIncludedIndex;
                        var result = await _clusterClient.Send(node.Key, new InstallSnapshot()
                        {
                            Term = _nodeStorage.CurrentTerm,
                            LastIncludedIndex = lastIndex,
                            LastIncludedTerm = _nodeStorage.LastSnapshotIncludedTerm,
                            LeaderId = _nodeStorage.Id,
                            Snapshot = _nodeStorage.LastSnapshot
                        });

                        if (result.IsSuccessful)
                        {
                            //mark the node has successfully received the logs
                            LogsSent.TryUpdate(node.Key, false, true);
                            NodeStateService.NextIndex[node.Key] = lastIndex + 1;
                        }

                    }

                    Interlocked.Increment(ref recognizedhosts);
                }
                catch (HttpRequestException e)
                {
                    unreachableNodes.Add(node.Key);
                    Logger.LogWarning(NodeStateService.GetNodeLogId() + "Heartbeat to node " + node.Key + " had a connection issue.");
                }
                catch (TaskCanceledException e)
                {
                    unreachableNodes.Add(node.Key);
                    Logger.LogWarning(NodeStateService.GetNodeLogId() + "Heartbeat to node " + node.Key + " timed out");
                }
                catch (Exception e)
                {
                    unreachableNodes.Add(node.Key);
                    Logger.LogWarning(NodeStateService.GetNodeLogId() + "Encountered error while sending heartbeat to node " + node.Key + ", request failed with error \"" + e.GetType().Name + e.Message + Environment.NewLine + e.StackTrace);//+ "\"" + e.StackTrace);
                }
            });

            await Task.WhenAll(tasks);

            //If less then the required number recognize the request, go back to being a candidate
            if (recognizedhosts < ClusterOptions.MinimumNodes)
            {
                SetNodeRole(NodeState.Candidate);
            }
            else
            {
                RemoveNodesFromCluster(unreachableNodes.ToList());
            }
        }

        public async void RemoveNodesFromCluster(IEnumerable<Guid> nodesToMarkAsStale)
        {
            var nodeUpsertCommands = new List<BaseCommand>();

            foreach (var nodeId in nodesToMarkAsStale)
            {
                //There could be already requests in queue marking the node as unreachable
                if (StateMachine.IsNodeContactable(nodeId))
                {
                    nodeUpsertCommands.Add(new DeleteNodeInformation()
                    {
                        Id = nodeId
                    });
                }
                _clusterConnectionPool.RemoveClient(nodeId);
            }
            if (nodeUpsertCommands.Count() > 0)
            {
                await Handle(new ExecuteCommands()
                {
                    Commands = nodeUpsertCommands,
                    WaitForCommits = true
                });
            }
        }

        public async void AddNodesToCluster(IEnumerable<NodeInformation> nodesInfo)
        {
            var nodeUpsertCommands = new List<BaseCommand>();

            foreach (var nodeInfo in nodesInfo)
            {
                nodeUpsertCommands.Add(new UpsertNodeInformation()
                {
                    Id = nodeInfo.Id,
                    Name = "",
                    TransportAddress = nodeInfo.TransportAddress,
                    IsContactable = true
                });
                _clusterConnectionPool.AddClient(nodeInfo.Id, nodeInfo.TransportAddress);
            }

            if (nodeUpsertCommands.Count() > 0)
            {
                await Handle(new ExecuteCommands()
                {
                    Commands = nodeUpsertCommands,
                    WaitForCommits = true
                });
            }
        }

    }
}
