using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.RPCs;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.SystemCommands;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node
{
    public class ConsensusCoreNode<State, Repository> : IConsensusCoreNode<State, Repository>
        where State : BaseState, new()
        where Repository : BaseRepository
    {
        private Timer _heartbeatTimer;
        private Timer _electionTimeoutTimer;
        private Timer _clusterInfoTimeoutTimer;

        private Thread _commitThread;

        private NodeOptions _nodeOptions { get; }
        private ClusterOptions _clusterOptions { get; }
        private NodeStorage _nodeStorage { get; }
        public NodeState CurrentState { get; private set; }
        public ILogger<ConsensusCoreNode<State, Repository>> Logger { get; }

        public Dictionary<string, HttpNodeConnector> NodeConnectors { get; private set; } = new Dictionary<string, HttpNodeConnector>();
        public Dictionary<string, int> NextIndex { get; private set; } = new Dictionary<string, int>();
        public ConcurrentDictionary<string, int> MatchIndex { get; private set; } = new ConcurrentDictionary<string, int>();
        public object matchIndexLock = new object();

        public StateMachine<State> _stateMachine { get; private set; }
        public string MyUrl { get; private set; }
        public bool IsBootstrapped = false;
        public KeyValuePair<Guid?, string> CurrentLeader;

        public Thread BootstrapThread;
        private Thread _findLeaderThread;

        private int maxSendEntries = 10000;

        ConcurrentQueue<LogEntry> cachedCommands = new ConcurrentQueue<LogEntry>();

        public IDataRouter _dataRouter;
        public bool enableDataRouting = false;

        /// <summary>
        /// What logs have been commited to the state
        /// </summary>
        public int CommitIndex { get; private set; }
        public NodeInfo NodeInfo
        {
            get
            {
                return new NodeInfo()
                {
                    Id = _nodeStorage.Id
                };
            }
        }

        public ConsensusCoreNode(
           IOptions<ClusterOptions> clusterOptions,
            IOptions<NodeOptions> nodeOptions,
            NodeStorage nodeStorage,
            ILogger<ConsensusCoreNode<
            State,
            Repository>> logger,
            StateMachine<State> stateMachine,
            IDataRouter dataRouter = null)
        {
            _nodeOptions = nodeOptions.Value;
            _clusterOptions = clusterOptions.Value;
            _nodeStorage = nodeStorage;
            Logger = logger;
            _electionTimeoutTimer = new Timer(ElectionTimeoutEventHandler);
            _heartbeatTimer = new Timer(HeartbeatTimeoutEventHandler);
            _clusterInfoTimeoutTimer = new Timer(ClusterInfoTimeoutHandler);
            _stateMachine = stateMachine;
            SetNodeRole(NodeState.Follower);

            BootstrapThread = new Thread(() =>
            {
                //Wait for the rest of the node to bootup
                Thread.Sleep(3000);
                BootstrapNode();
            });

            BootstrapThread.Start();

            _dataRouter = dataRouter;
            if (dataRouter != null)
            {
                Logger.LogDebug("Data routing has been enabled on this node");
                enableDataRouting = true;
            }
        }

        public void ResetLeaderState()
        {
            NextIndex.Clear();
            MatchIndex.Clear();
            foreach (var url in _clusterOptions.NodeUrls)
            {
                NextIndex.Add(url, _nodeStorage.GetLogCount() + 1);
                MatchIndex.TryAdd(url, 0);
            }
        }

        public Thread NewCommitThread()
        {
            return new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                while (CurrentState == NodeState.Leader)
                {
                    while (CommitIndex < _nodeStorage.GetLastLogIndex())
                    {
                        if (MatchIndex.Values.Count(x => x >= CommitIndex + 1) >= (_clusterOptions.MinimumNodes - 1))
                        {
                            _stateMachine.ApplyLogToStateMachine(_nodeStorage.GetLogAtIndex(CommitIndex + 1));
                            CommitIndex++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    Thread.Sleep(10);
                }
            });
        }

        public Thread FindLeaderThread(Guid id)
        {
            return new Thread(() =>
            {
                while (CurrentLeader.Value == null)
                {
                    var matchingNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Key == id);
                    var node = matchingNodes.Count() == 1 ? matchingNodes.First().Value : null;

                    if (node == null)
                    {
                        Logger.LogWarning("Leader was not found in cluster, routing via this node may fail... will sleep and try again..");
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        Logger.LogDebug("Leader was found at URL " + node.TransportAddress);
                        CurrentLeader = new KeyValuePair<Guid?, string>(id, node.TransportAddress);
                    }
                }
            });
        }

        public void BootstrapNode()
        {
            Logger.LogInformation("Bootstrapping Node!");

            // The node cannot bootstrap unless at least a majority of nodes are present

            while (NodeConnectors.Count() < _clusterOptions.MinimumNodes - 1)
            {
                NodeConnectors.Clear();
                foreach (var url in _clusterOptions.NodeUrls)
                {
                    var testConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs));

                    Guid? nodeId = null;
                    try
                    {
                        nodeId = testConnector.GetNodeInfoAsync().GetAwaiter().GetResult().Id;

                        if (nodeId != _nodeStorage.Id)
                        {
                            NodeConnectors.Add(url, testConnector);
                        }
                        else
                        {
                            MyUrl = url;
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Node at url " + url + " was unreachable...");
                    }
                }

                if (MyUrl == null)
                {
                    Logger.LogWarning("Node is not discoverable from the given node urls!");
                }

                if (NodeConnectors.Count() < _clusterOptions.MinimumNodes - 1)
                {
                    Logger.LogWarning("Not enough of the nodes in the cluster are contactable, awaiting bootstrap");
                }
            }

            IsBootstrapped = true;
        }

        #region Timeout Handlers
        public void ClusterInfoTimeoutHandler(object args)
        {
            Logger.LogDebug("Rediscovering nodes...");
            var nodeUpsertCommands = new List<BaseCommand>();
            var nodesAreMissing = false;
            do
            {
                nodeUpsertCommands = new List<BaseCommand>();
                foreach (var url in _clusterOptions.NodeUrls)
                {

                    Guid? nodeId = null;
                    try
                    {
                        var tempConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs));
                        nodeId = tempConnector.GetNodeInfoAsync().GetAwaiter().GetResult().Id;

                        var possibleNodeUpdate = new NodeInformation()
                        {
                            Name = "",
                            TransportAddress = url
                        };

                        //If the node does not exist
                        if ((!_stateMachine.CurrentState.Nodes.ContainsKey(nodeId.Value) ||
                            // Check whether the node with the same id has different information
                            !_stateMachine.CurrentState.Nodes[nodeId.Value].Equals(possibleNodeUpdate))
                            && nodeId.Value != null
                            )
                        {
                            Logger.LogDebug("Detected updated for node " + nodeId);
                            nodeUpsertCommands.Add((BaseCommand)new UpsertNodeInformation()
                            {
                                Id = nodeId.Value,
                                Name = "",
                                TransportAddress = url
                            });

                            var conflictingNodes = _stateMachine.CurrentState.Nodes.Where(v => v.Value.TransportAddress == url && v.Key != nodeId);
                            // If there is another current node with that transport address
                            if (conflictingNodes.Count() == 1)
                            {
                                Logger.LogWarning("Detected another node with conflicting transport address, removing the target node");
                                nodeUpsertCommands.Add(new DeleteNodeInformation()
                                {
                                    Id = conflictingNodes.First().Key
                                });
                            }
                        }

                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Node at url " + url + " was unreachable...");
                        nodesAreMissing = true;
                    }
                }
            }
            while (nodesAreMissing && CurrentState == NodeState.Leader);

            if (CurrentState == NodeState.Leader)
                if (nodeUpsertCommands.Count > 0)
                {
                    Send(new ExecuteCommands()
                    {
                        Commands = nodeUpsertCommands,
                        WaitForCommits = true
                    });
                }
        }

        public void ElectionTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                Logger.LogDebug("Detected election timeout event.");
                _nodeStorage.UpdateCurrentTerm(_nodeStorage.CurrentTerm + 1);
                SetNodeRole(NodeState.Candidate);

                var totalVotes = 1;

                Parallel.ForEach(NodeConnectors, connector =>
                {
                    try
                    {
                        var result = connector.Value.Send(new RequestVote()
                        {
                            Term = _nodeStorage.CurrentTerm,
                            CandidateId = _nodeStorage.Id,
                            LastLogIndex = _nodeStorage.GetLastLogIndex(),
                            LastLogTerm = _nodeStorage.GetLastLogTerm()
                        }).GetAwaiter().GetResult();

                        if (result.Success)
                        {
                            Interlocked.Increment(ref totalVotes);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Encountered error while getting vote from node " + connector.Key + ", request failed with error \"" + e.Message + "\"");
                    }
                });

                if (totalVotes >= _clusterOptions.MinimumNodes)
                {
                    Logger.LogInformation("Recieved enough votes to be promoted, promoting to leader.");
                    SetNodeRole(NodeState.Leader);
                }
            }
        }

        public void HeartbeatTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                Logger.LogDebug("Detected heartbeat timeout event.");
                SendHeartbeats();
            }
        }
        #endregion

        /// <summary>
        /// Find nodes based on the url
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public string FindNodeUrl(Guid id)
        {
            var matchingNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Key == id);
            var node = matchingNodes.Count() == 1 ? matchingNodes.First().Value : null;

            if (node == null)
            {
                Logger.LogWarning("Could not find node " + id.ToString());
                return "";
            }
            else
            {
                return node.TransportAddress;
            }
        }

        #region RPC Handlers

        public TResponse Send<TResponse>(IClusterRequest<TResponse> request)
        {
            Logger.LogDebug("Detected RPC " + request.GetType().Name + ".");
            if (!IsBootstrapped)
            {
                Logger.LogDebug("Node is not ready...");
                return default(TResponse);
            }

            switch (request)
            {
                case ExecuteCommands t1:
                    return HandleIfLeaderOrReroute(request, () => (TResponse)(object)ExecuteCommandsRPCHandler(t1));
                case WriteDataShard t1:
                    return (TResponse)(object)WriteDataShardRPCHandler(t1);
                case RequestVote t1:
                    return (TResponse)(object)RequestVoteRPCHandler(t1);
                case AppendEntry t1:
                    return (TResponse)(object)AppendEntryRPCHandler(t1);
                case RequestShardAllocationUpdate t1:
                    return (TResponse)(object)RequestShardAllocationUpdateHandler(t1);
                case RequestDataShard t1:
                    return (TResponse)(object)RequestDataShardHandler(t1);
                case AssignNewShard t1:
                    return HandleIfLeaderOrReroute(request, () => (TResponse)(object)AssignNewShardHandler(t1));
            }

            throw new Exception("Request is not implemented");
        }

        public TResponse HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<TResponse> Handle)
        {
            // if you change and become a leader, just handle this yourself.
            while (CurrentState != NodeState.Leader)
            {
                try
                {
                    return (TResponse)(object)GetLeadersConnector().Send(request).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    Logger.LogDebug("Encountered " + e.Message + " while trying to route " + request.GetType().Name + " to leader.");
                }
            }
            return Handle();
        }

        public ExecuteCommandsResponse ExecuteCommandsRPCHandler(ExecuteCommands request)
        {
            int index = _nodeStorage.AddLog(request.Commands.ToList(), _nodeStorage.CurrentTerm);

            while (request.WaitForCommits)
            {
                Logger.LogDebug("Waiting for " + request.RequestName + " to complete.");
                if (CommitIndex >= index)
                {
                    return new ExecuteCommandsResponse()
                    {
                        IsSuccessful = true
                    };
                }
                else
                {
                    Thread.Sleep(10);
                }
            }

            return new ExecuteCommandsResponse()
            {
                IsSuccessful = true
            };
        }

        public RequestShardAllocationUpdateResponse RequestShardAllocationUpdateHandler(RequestShardAllocationUpdate request)
        {
            var response = Send(new ExecuteCommands()
            {
                Commands = new List<BaseCommand>(){new UpdateShardAllocation()
                {
                    NodeId = request.NodeId,
                    ShardId = request.ShardId,
                    Version = request.Version
                } },
                WaitForCommits = false
            });

            return new RequestShardAllocationUpdateResponse()
            {
                IsSuccessful = response.IsSuccessful
            };
        }

        public AssignNewShardResponse AssignNewShardHandler(AssignNewShard shard)
        {
            AssignShard(shard.Type, shard.ShardId);
            return new AssignNewShardResponse()
            {
                IsSuccessful = true
            };
        }

        /// <summary>
        /// Initial update should occur on the primary node which will then replicate it to the rest of the cluster.
        /// </summary>
        /// <param name="shard"></param>
        /// <returns></returns>
        public WriteDataShardResponse WriteDataShardRPCHandler(WriteDataShard shard)
        {
            // If there is no shardId, this means the request is a new shard
            if (shard.ShardId == null || !_stateMachine.ShardExists(shard.ShardId.Value))
            {
                Logger.LogDebug("Detected request to create a new shard of type " + shard.Type + ".");

                if (shard.ShardId == null)
                    shard.ShardId = Guid.NewGuid();

                Send(new AssignNewShard()
                {
                    ShardId = shard.ShardId.Value,
                    Type = shard.Type
                });
            }

            var processStartTime = DateTime.UtcNow;
            while (!_stateMachine.ShardExists(shard.ShardId.Value))
            {
                Thread.Sleep(100);
                Logger.LogDebug("Awaiting assignment of shard " + shard.ShardId + " of type " + shard.Type + ".");

                if ((DateTime.UtcNow - processStartTime).TotalMilliseconds > _clusterOptions.LatencyToleranceMs)
                {
                    return new WriteDataShardResponse()
                    {
                        IsSuccessful = false
                    };
                }
            }

            if (shard.Version == null)
            {
                Logger.LogWarning("Shard , changing version to 1.");
                //Increment the version if it hasn't been incremented
                shard.Version = _stateMachine.GetLatestShardVersion(shard.ShardId.Value) + 1;
            }

            //If you are the primary
            if (_stateMachine.ShardIsPrimaryOnNode(shard.ShardId.Value, _nodeStorage.Id))
            {
                //Send all the data to the replicas
                var saveResult = _dataRouter.WriteData(shard.Type, shard.ShardId.Value, shard.Data);

                Send(new RequestShardAllocationUpdate()
                {
                    NodeId = _nodeStorage.Id,
                    ShardId = shard.ShardId.Value,
                    Version = shard.Version.Value
                });

                //Queue up the replication tasks    
            }
            else
            {
                //Reroute the initial request to the primary
                try
                {
                    // Try to send to the primary, otherwise you will have to change the primary and retry
                    NodeConnectors[_stateMachine.CurrentState.Nodes[_stateMachine.GetShardPrimaryNode(shard.ShardId.Value)].TransportAddress].Send(shard).GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to write the shard to the primary, TODO, reassign primary and try again with new primary" + e.Message);
                }
            }

            return new WriteDataShardResponse()
            {
                IsSuccessful = true
            };
        }

        public RequestVoteResponse RequestVoteRPCHandler(RequestVote requestVoteRPC)
        {
            var successful = false;
            if (IsBootstrapped)
            {
                //Ref1 $5.2, $5.4
                if (_nodeStorage.CurrentTerm < requestVoteRPC.Term && ((_nodeStorage.VotedFor == null || _nodeStorage.VotedFor == requestVoteRPC.CandidateId) &&
                (requestVoteRPC.LastLogIndex >= _nodeStorage.GetLogCount() - 1 && requestVoteRPC.LastLogTerm >= _nodeStorage.GetLastLogTerm())))
                {
                    _nodeStorage.SetVotedFor(requestVoteRPC.CandidateId);
                    Logger.LogInformation("Voting for " + requestVoteRPC.CandidateId + " for term " + requestVoteRPC.Term);
                    SetNodeRole(NodeState.Follower);
                    successful = true;
                }
            }
            return new RequestVoteResponse()
            {
                Success = successful
            };
        }

        public AppendEntryResponse AppendEntryRPCHandler(AppendEntry entry)
        {
            ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
            if (entry.Term < _nodeStorage.CurrentTerm)
            {
                return new AppendEntryResponse()
                {
                    Successful = false
                };
            }

            var previousEntry = _nodeStorage.GetLogAtIndex(entry.PrevLogIndex);

            if (previousEntry == null && entry.PrevLogIndex != 0)
            {
                Logger.LogWarning("Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");

                return new AppendEntryResponse()
                {
                    Successful = false,
                    ConflictingTerm = null,
                    ConflictName = AppendEntriesExceptionNames.MissingLogEntryException,
                    FirstTermIndex = null,
                    LastLogEntryIndex = _nodeStorage.GetLogCount()
                };
            }

            if (previousEntry != null && previousEntry.Term != entry.PrevLogTerm)
            {
                Logger.LogWarning("Inconsistency found in the node logs and leaders logs, log " + entry.PrevLogTerm + " from term " + entry.PrevLogTerm + " does not exist.");

                return new AppendEntryResponse()
                {
                    Successful = false,
                    ConflictingTerm = entry.PrevLogTerm,
                    FirstTermIndex = _nodeStorage.Logs.Where(l => l.Term == entry.PrevLogTerm).First().Index
                };
            }

            foreach (var log in entry.Entries.OrderBy(e => e.Index))
            {
                var existingEnty = _nodeStorage.GetLogAtIndex(log.Index);
                if (existingEnty != null && existingEnty.Term != log.Term)
                {
                    _nodeStorage.DeleteLogsFromIndex(log.Index);
                    break;
                }
            }

            if (CurrentLeader.Key != entry.LeaderId)
            {
                Logger.LogDebug("Detected uncontacted leader, discovering leader now.");
                //Reset the current leader
                CurrentLeader = new KeyValuePair<Guid?, string>(entry.LeaderId, null);
                _findLeaderThread = FindLeaderThread(entry.LeaderId);
                _findLeaderThread.Start();
            }

            SetNodeRole(NodeState.Follower);

            foreach (var log in entry.Entries)
            {
                _nodeStorage.AddLog(log.Commands, log.Term);
            }

            if (CommitIndex < entry.LeaderCommit)
            {
                Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                var allLogsToBeCommited = _nodeStorage.Logs.Where(l => l.Index > CommitIndex && l.Index <= entry.LeaderCommit);
                _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                CommitIndex = entry.LeaderCommit;
            }
            return new AppendEntryResponse()
            {
                Successful = true
            };
        }

        public RequestDataShardResponse RequestDataShardHandler(RequestDataShard request)
        {
            bool found = false;
            RequestDataShardResponse data = null;
            //If the data is stored here, then fetch it from here
            if (_stateMachine.NodeHasShardLatestVersion(_nodeStorage.Id, request.ShardId))
            {
                return new RequestDataShardResponse()
                {
                    Data = _dataRouter.GetData(request.Type, request.ShardId)
                };
            }
            // else route the request to all insync-nodes
            else
            {
                //Implement routing logic

                var allocatedNodes = _stateMachine.AllNodesWithUptoDateShard(request.ShardId);

                Parallel.ForEach(allocatedNodes, node =>
                        {
                            try
                            {
                                var nodeInformation = _stateMachine.CurrentState.Nodes[node];

                                //All data returned from the cluster will be the same
                                data = NodeConnectors[nodeInformation.TransportAddress].Send(request).GetAwaiter().GetResult();

                                found = true;
                            }
                            catch (Exception e)
                            {
                                Logger.LogError("Failed to retrieve the data from node " + node + " for shard " + request.ShardId);
                            }
                        });
            }

            while (!found)
            {
                Logger.LogDebug("Waiting for a node to respond with data for shard " + request.ShardId);
                Thread.Sleep(100);
            }

            return data;
        }

        #endregion

        #region Internal Parallel Calls
        public void SendHeartbeats()
        {
            Logger.LogDebug("Sending heartbeats");
            Parallel.ForEach(NodeConnectors, connector =>
            {
                try
                {
                    var entriesToSend = new List<LogEntry>();

                    var prevLogIndex = Math.Max(0, NextIndex[connector.Key] - 1);
                    int prevLogTerm = (_nodeStorage.GetLogCount() > 0 && prevLogIndex > 0) ? prevLogTerm = _nodeStorage.GetLogAtIndex(prevLogIndex).Term : 0;

                    if (NextIndex[connector.Key] <= _nodeStorage.GetLastLogIndex() && _nodeStorage.GetLastLogIndex() != 0)
                    {
                        var quantityToSend = (_nodeStorage.GetLogCount() - NextIndex[connector.Key] + 1);
                        entriesToSend = _nodeStorage.Logs.GetRange(NextIndex[connector.Key] - 1, quantityToSend < maxSendEntries ? quantityToSend : maxSendEntries).ToList();
                        // entriesToSend = _nodeStorage.Logs.Where(l => l.Index >= NextIndex[connector.Key]).ToList();
                        Logger.LogDebug("Detected node " + connector.Key + " is not upto date, sending logs from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index);
                    }

                    var result = connector.Value.Send(new AppendEntry()
                    {
                        Term = _nodeStorage.CurrentTerm,
                        Entries = entriesToSend,
                        LeaderCommit = CommitIndex,
                        LeaderId = _nodeStorage.Id,
                        PrevLogIndex = prevLogIndex,
                        PrevLogTerm = prevLogTerm
                    }).GetAwaiter().GetResult();

                    if (result.Successful)
                    {
                        if (entriesToSend.Count() > 0)
                        {
                            NextIndex[connector.Key] = entriesToSend.Last().Index + 1;

                            int previousValue;
                            MatchIndex.TryGetValue(connector.Key, out previousValue);
                            MatchIndex.TryUpdate(connector.Key, entriesToSend.Last().Index, previousValue);
                        }
                    }
                    else if (result.ConflictName == AppendEntriesExceptionNames.MissingLogEntryException)
                    {
                        Logger.LogWarning("Detected node " + connector.Value + " is missing the previous log, sending logs from log " + result.LastLogEntryIndex.Value + 1);
                        NextIndex[connector.Key] = result.LastLogEntryIndex.Value + 1;
                    }
                    else if (result.ConflictName == AppendEntriesExceptionNames.ConflictingLogEntryException)
                    {
                        var firstEntryOfTerm = _nodeStorage.Logs.Where(l => l.Term == result.ConflictingTerm).FirstOrDefault();
                        var revertedIndex = firstEntryOfTerm.Index < result.FirstTermIndex ? firstEntryOfTerm.Index : result.FirstTermIndex.Value;
                        Logger.LogWarning("Detected node " + connector.Value + " has conflicting values, reverting to " + revertedIndex);

                        //Revert back to the first index of that term
                        NextIndex[connector.Key] = revertedIndex;
                    }
                    else
                    {
                        throw new Exception("Append entry returned with undefined conflict name");
                    }
                }
                catch (Exception e)
                {
                    Logger.LogWarning("Encountered error while sending heartbeat to node " + connector.Key + ", request failed with error \"" + e.Message + "\"" + e.StackTrace);
                }
            });
        }

        public void SetNodeRole(NodeState newState)
        {
            if (newState != CurrentState)
            {
                Logger.LogInformation("Node's role changed to " + newState.ToString());
                CurrentState = newState;

                switch (newState)
                {
                    case NodeState.Candidate:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        break;
                    case NodeState.Follower:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        break;
                    case NodeState.Leader:
                        CurrentLeader = new KeyValuePair<Guid?, string>(_nodeStorage.Id, MyUrl);
                        ResetLeaderState();
                        ResetTimer(_heartbeatTimer, _clusterOptions.ElectionTimeoutMs / 2, _clusterOptions.ElectionTimeoutMs / 2);
                        StopTimer(_electionTimeoutTimer);
                        _commitThread = NewCommitThread();
                        _commitThread.Start();
                        ResetTimer(_clusterInfoTimeoutTimer, 0, 1000);
                        break;
                }
            }
        }
        #endregion

        private void ResetTimer(Timer timer, int dueTime, int period)
        {
            timer.Change(dueTime, period);
        }

        private void StopTimer(Timer timer)
        {
            ResetTimer(timer, Timeout.Infinite, Timeout.Infinite);
        }

        public State GetState()
        {
            return _stateMachine.CurrentState;
        }

        public HttpNodeConnector GetLeadersConnector()
        {
            if (!_stateMachine.CurrentState.Nodes.ContainsKey(CurrentLeader.Key.Value))
                NodeConnectors.Add(CurrentLeader.Value, new HttpNodeConnector(CurrentLeader.Value, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs)));
            return NodeConnectors[CurrentLeader.Value];
        }



        /// <summary>
        /// Decide who should be the nodes storing the data
        /// </summary>
        /// <param name="type"></param>
        /// <param name="shardId"></param>
        public void AssignShard(string type, Guid shardId)
        {
            bool successfulAllocation = false;
            while (!successfulAllocation)
            {
                try
                {
                    //This is for the primary copy
                    var eligbleNodes = _stateMachine.CurrentState.Nodes;
                    var rand = new Random();
                    var selectedNode = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count()));
                    Logger.LogDebug("Allocating data shart to node at " + selectedNode.Key);

                    Send(new ExecuteCommands()
                    {
                        Commands = new List<UpsertDataShardInformation>() {
                                new UpsertDataShardInformation() {
                                        InsyncAllocations = new Guid[] { selectedNode.Key },
                                        ShardId = shardId,
                                        PrimaryAllocation = selectedNode.Key,
                                        Allocations = _stateMachine.CurrentState.Nodes.Select(n => n.Key).ToDictionary(
                                            key => key,
                                            value => 0
                                            ),
                                        Type = type,
                                        //Set to 0 to show uninitalized shard
                                        Version = 0,
                                        Initalized = false
                                }
                            },
                        WaitForCommits = false
                    });
                    successfulAllocation = true;
                }
                catch (Exception e)
                {
                    Logger.LogDebug("Error while assigning primary node " + e.StackTrace);
                }
            }
        }

        /*public void UpdateShardVersion(string type, Guid shardId)
        {
            if (CurrentState == NodeState.Leader)
            {
                bool successfulAllocation = false;
                while (!successfulAllocation)
                {
                    try
                    {
                        //This is for the primary copy
                        var eligbleNodes = _stateMachine.CurrentState.Nodes;
                        var rand = new Random();
                        var selectedNode = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count() - 1));
                        Logger.LogDebug("Allocating data shart to node at " + selectedNode.Key);

                        Send(new ExecuteCommands()
                        {
                            Commands = new List<UpsertDataShardInformation>() {
                                new UpsertDataShardInformation() {
                                        InsyncAllocations = new Guid[] { selectedNode.Key },
                                        ShardId = shardId,
                                        PrimaryAllocation = selectedNode.Key,
                                        Allocations = _stateMachine.CurrentState.Nodes.Select(n => n.Key).ToDictionary(
                                            key => key,
                                            value => 0
                                            ),
                                        Type = type,
                                        //Set to 0 to show uninitalized shard
                                        Version = 0,
                                        Initalized = false
                                }
                            },
                            WaitForCommits = true
                        });
                        successfulAllocation = true;
                    }
                    catch (Exception e)
                    {
                        Logger.LogDebug("Error while assigning primary node " + e.StackTrace);
                    }
                }
            }
            // If you are a follower, 
            else if (CurrentState == NodeState.Follower)
            {
                //Route to a corresponding node
            }
        }*/
    }
}