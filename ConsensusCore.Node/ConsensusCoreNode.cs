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
        public Dictionary<string, int> MatchIndex { get; private set; } = new Dictionary<string, int>();

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
                MatchIndex.Add(url, 0);
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
                    var testConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs));

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
                        var tempConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(10000));
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
            while (nodesAreMissing);


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
            switch (request)
            {
                case ExecuteCommands t1:
                    return (TResponse)(object)ExecuteCommandsRPCHandler((ExecuteCommands)request);
                case WriteDataShard t1:
                    return (TResponse)(object)WriteDataShardRPCHandler((WriteDataShard)request);
                case RequestVote t1:
                    return (TResponse)(object)RequestVoteRPCHandler((RequestVote)request);
                case AppendEntry t1:
                    return (TResponse)(object)AppendEntryRPCHandler((AppendEntry)request);
            }

            throw new Exception("Request is not implemented");
        }

        public bool ExecuteCommandsRPCHandler(ExecuteCommands request)
        {
            if (CurrentState == NodeState.Leader)
            {
                int index = _nodeStorage.AddLog(request.Commands.ToList(), _nodeStorage.CurrentTerm);

                while (request.WaitForCommits)
                {
                    if (CommitIndex >= index)
                    {
                        return true;
                    }
                    else
                    {
                        Thread.Sleep(10);
                    }
                }

                return true;
            }
            else if (CurrentState == NodeState.Follower)
            {
                Logger.LogDebug("Recieved command but current state is " + CurrentState.ToString() + " routing to leader " + CurrentLeader.Key);
                //Once you become a follower you need to be able to contact the leader
                if (_stateMachine.CurrentState.Nodes.ContainsKey(CurrentLeader.Key.Value))
                    NodeConnectors.Add(CurrentLeader.Value, new HttpNodeConnector(CurrentLeader.Value, TimeSpan.FromMilliseconds(10000)));
                GetLeadersConnector().Send(request).GetAwaiter().GetResult();
                return true;
            }
            else
            {
                //TO DO, cache this request
                Logger.LogError("Received request as candidate...");
                return false;
            }
        }

        /* public Guid? CreateDataShardRPCHandler(CreateDataShard shard)
         {
             // If you are leader, figure out who is to be assigned the shard and send the data to all valid candidates
             if (CurrentState == NodeState.Leader)
             {
                 bool successfulAllocation = false;
                 Guid shardId = Guid.NewGuid();
                 while (!successfulAllocation)
                 {
                     try
                     {
                         var eligbleNodes = _stateMachine.CurrentState.Nodes;
                         var rand = new Random();
                         var selectedNode = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count() - 1));
                         Logger.LogDebug("Allocating data shart to node at " + selectedNode.Key);

                         //If it is not this node
                         if (selectedNode.Key != _nodeStorage.Id)
                         {
                             var result = NodeConnectors[selectedNode.Value.TransportAddress].AssignShardAsync(new Models.AssignDataShard()
                             {
                                 ShardId = shardId,
                                 Data = shard.Data,
                                 Type = shard.Type,
                                 Term = _nodeStorage.CurrentTerm,
                                 LeaderId = _nodeStorage.Id,
                                 Version = 1
                             }).GetAwaiter().GetResult();
                         }
                         //Append it to youself
                         else
                         {
                             Send(new UpdateDataShard()
                             {
                                 ShardId = shardId,
                                 Data = shard.Data,
                                 Type = shard.Type,
                                 Term = _nodeStorage.CurrentTerm,
                                 LeaderId = _nodeStorage.Id,
                                 Version = 1
                             });
                         }

                         Send(new ExecuteCommands()
                         {
                             Commands = new List<UpsertDataShardInformation>() {
                             new UpsertDataShardInformation() {
                                 InsyncAllocations = new Guid[] { selectedNode.Key },
                                 ShardId = shardId,
                                 PrimaryAllocation = selectedNode.Key,
                                 Type = shard.Type,
                                 Version = 1
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

                 return shardId;
             }
             // If you are a follower, 
             else if (CurrentState == NodeState.Follower)
             {
                 //Route to a corresponding node
             }

             return null;
         }*/

        /// <summary>
        /// Initial update should occur on the primary node which will then replicate it to the rest of the cluster.
        /// </summary>
        /// <param name="shard"></param>
        /// <returns></returns>
        public bool WriteDataShardRPCHandler(WriteDataShard shard)
        {
            // If there is no shardId, this means the request is a new shard
            if (shard.ShardId == null || !_stateMachine.ShardExists(shard.ShardId.Value))
            {
                Logger.LogDebug("Detected request to create a new shard of type " + shard.Type + ".");

                if (CurrentState == NodeState.Leader)
                {
                    if (shard.ShardId == null)
                        shard.ShardId = Guid.NewGuid();
                    // Assign this shard to the cluster
                    AssignShard(shard.Type, shard.ShardId.Value);
                }
                else if (CurrentState == NodeState.Follower)
                {
                    //Route this to the leader
                }

                if (shard.Version != 1)
                {
                    Logger.LogWarning("Shard was detected to be a new write however version was not 1, changing version to 1.");
                    shard.Version = 1;
                }
            }

            while (!_stateMachine.ShardExists(shard.ShardId.Value))
            {
                Thread.Sleep(100);
                Logger.LogDebug("Awaiting assignment of shard " + shard.ShardId + " of type " + shard.Type + ".");
            }

            // Write the shard to disk if you are apart of the allocations
            if (_stateMachine.ShardIsAssignedToNode(shard.ShardId.Value, _nodeStorage.Id))
            {
                if (_stateMachine.NodeHasOlderShard(_nodeStorage.Id, shard.ShardId.Value, shard.Version))
                {
                    var saveResult = _dataRouter.SaveData(shard.Type, shard.ShardId.Value, shard.Data);
                    //If you are the leader, save the new state to disk otherwise tell the master
                }
            }

            //If you are the primary
            if (_stateMachine.ShardIsPrimaryOnNode(shard.ShardId.Value, _nodeStorage.Id))
            {
                //Send all the data to the replicas
            }

            return true;
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
            if (IsBootstrapped)
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

            throw new Exception("No AppendEntryResponse sent back.");
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
                    });

                    if (entriesToSend.Count() > 0)
                    {
                        NextIndex[connector.Key] = entriesToSend.Last().Index + 1;
                        MatchIndex[connector.Key] = entriesToSend.Last().Index;
                    }
                }
                catch (ConflictingLogEntryException e)
                {
                    var firstEntryOfTerm = _nodeStorage.Logs.Where(l => l.Term == e.ConflictingTerm).FirstOrDefault();
                    var revertedIndex = firstEntryOfTerm.Index < e.FirstTermIndex ? firstEntryOfTerm.Index : e.FirstTermIndex;
                    Logger.LogWarning("Detected node " + connector.Value + " has conflicting values, reverting to " + revertedIndex);

                    //Revert back to the first index of that term
                    NextIndex[connector.Key] = revertedIndex;
                }
                catch (MissingLogEntryException e)
                {
                    Logger.LogWarning("Detected node " + connector.Value + " is missing the previous log, sending logs from log " + e.LastLogEntryIndex + 1);
                    NextIndex[connector.Key] = e.LastLogEntryIndex + 1;
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
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        break;
                    case NodeState.Follower:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        break;
                    case NodeState.Leader:
                        CurrentLeader = new KeyValuePair<Guid?, string>(_nodeStorage.Id, MyUrl);
                        ResetLeaderState();
                        ResetTimer(_heartbeatTimer, _clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs);
                        StopTimer(_electionTimeoutTimer);
                        _commitThread = NewCommitThread();
                        _commitThread.Start();
                        ResetTimer(_clusterInfoTimeoutTimer, 0, 1000);
                        break;
                }
            }
        }

        /*public bool WriteDataShards(Guid id, string type, object data)
        {
            // If you are leader, figure out who is to be assigned the shard and send the data to all valid candidates
            if (CurrentState == NodeState.Leader)
            {
                bool successfulAllocation = false;
                while (!successfulAllocation)
                {
                    try
                    {
                        var primaryNodes = _stateMachine.CurrentState.Shards[id].InsyncAllocations;
                        //Dicitonary to keep track of which shards were successfully allocated
                        ConcurrentDictionary<Guid, bool> SuccessDict = new ConcurrentDictionary<Guid, bool>();

                        Parallel.ForEach(primaryNodes, node =>
                        {
                            try
                            {
                                var nodeInformation = _stateMachine.CurrentState.Nodes[node];
                                //If it is not this node
                                if (node != _nodeStorage.Id)
                                {
                                    var result = NodeConnectors[nodeInformation.TransportAddress].AssignShardAsync(new Models.AssignDataShard()
                                    {
                                        ShardId = id,
                                        Data = data,
                                        Type = type,
                                        Term = _nodeStorage.CurrentTerm,
                                        LeaderId = _nodeStorage.Id,
                                        Version = _stateMachine.CurrentState.Shards[id].Version + 1
                                    }).GetAwaiter().GetResult();
                                }
                                //Append it to youself
                                else
                                {
                                    UpdateDataShard(new Models.AssignDataShard()
                                    {
                                        ShardId = id,
                                        Data = data,
                                        Type = type,
                                        Term = _nodeStorage.CurrentTerm,
                                        LeaderId = _nodeStorage.Id,
                                        Version = _stateMachine.CurrentState.Shards[id].Version + 1
                                    });
                                }

                                SuccessDict.TryAdd(node, true);
                            }
                            catch (Exception e)
                            {
                                Logger.LogError("Unable to allocated data to node " + node + " due to error " + e.StackTrace);
                                SuccessDict.TryAdd(node, false);
                            }
                        });

                        //Wait for all the update requests to complete
                        while (SuccessDict.Count() < primaryNodes.Length)
                        {
                            Logger.LogDebug("Update " + id + "has not been completed, " + SuccessDict.Count() + "/" + primaryNodes.Length + " completed.");
                            Thread.Sleep(1000);
                        }

                        var allSuccessfullNodes = SuccessDict.Where(s => s.Value).Select(sd => sd.Key).ToArray();

                        if (allSuccessfullNodes.Count() == 0)
                        {
                            //Fix this
                            throw new Exception("No nodes were available for this write operation.");
                        }

                        var rand = new Random();

                        ProcessCommandsRequestHandler(new ProcessCommandsRequest()
                        {
                            Commands =
                            new List<UpsertDataShardInformation>() {
                            new UpsertDataShardInformation() {
                                //Only get the insync allocations
                                InsyncAllocations = allSuccessfullNodes,
                                ShardId = id,
                                PrimaryAllocation = allSuccessfullNodes[rand.Next(0, allSuccessfullNodes.Length - 1)],
                                Type = type,
                                Version = _stateMachine.CurrentState.Shards[id].Version + 1
                            }
                        },
                            WaitForCommits = true
                        });
                        Logger.LogDebug("Successfully wrote update for shard " + id + " to cluster.");
                        successfulAllocation = true;
                    }
                    catch (Exception e)
                    {
                        Logger.LogDebug("Error while assigning primary node " + e.StackTrace);
                    }
                }

                return true;
            }
            // If you are a follower, 
            else if (CurrentState == NodeState.Follower)
            {
                //Route to a corresponding node
            }

            return false;
        }*/
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
            if (_stateMachine.CurrentState.Nodes.ContainsKey(CurrentLeader.Key.Value))
                NodeConnectors.Add(CurrentLeader.Value, new HttpNodeConnector(CurrentLeader.Value, TimeSpan.FromMilliseconds(10000)));
            return NodeConnectors[CurrentLeader.Value];
        }

        public object GetData(Guid id, string type)
        {
            bool found = false;
            object data = null;
            //If the data is stored here, then fetch it from here
            if (_stateMachine.NodeHasShardLatestVersion(_nodeStorage.Id, id))
            {
                return _dataRouter.GetData(type, id);
            }
            // else route the request to all insync-nodes
            else
            {
                //Implement routing logic

                var allocatedNodes = _stateMachine.AllNodesWithUptoDateShard(id);

                Parallel.ForEach(allocatedNodes, node =>
                {
                    try
                    {
                        var nodeInformation = _stateMachine.CurrentState.Nodes[node];

                        //All data returned from the cluster will be the same
                        //  data = NodeConnectors[nodeInformation.TransportAddress].GetDataShard(type, id);
                        found = true;
                    }
                    catch (Exception e)
                    {
                        Logger.LogError("Failed to retrieve the data from node " + node + " for shard " + id);
                    }
                });
            }

            while (!found)
            {
                Logger.LogDebug("Waiting for a node to respond with data for shard " + id);
                Thread.Sleep(100);
            }

            return data;
        }

        /// <summary>
        /// Decide who should be the nodes storing the data
        /// </summary>
        /// <param name="type"></param>
        /// <param name="shardId"></param>
        public void AssignShard(string type, Guid shardId)
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
        }

        public void UpdateShardVersion(string type, Guid shardId)
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
        }
    }
}