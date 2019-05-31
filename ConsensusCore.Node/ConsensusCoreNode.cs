using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.RPCs;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.SystemCommands;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
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
        private Thread _shardCreationThread;

        private Thread _commitThread;

        private NodeOptions _nodeOptions { get; }
        private ClusterOptions _clusterOptions { get; }
        private NodeStorage _nodeStorage { get; }
        public NodeState CurrentState { get; private set; }
        public ILogger<ConsensusCoreNode<State, Repository>> Logger { get; }

        public Dictionary<string, HttpNodeConnector> NodeConnectors { get; private set; } = new Dictionary<string, HttpNodeConnector>();
        public Dictionary<string, int> NextIndex { get; private set; } = new Dictionary<string, int>();
        public ConcurrentDictionary<string, int> MatchIndex { get; private set; } = new ConcurrentDictionary<string, int>();
        //Used to track whether you are currently already sending logs to a particular node to not double send
        public ConcurrentDictionary<string, bool> LogsSent = new ConcurrentDictionary<string, bool>();
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

        ConcurrentQueue<KeyValuePair<string, Guid>> DataAssignmentQueue { get; set; } = new ConcurrentQueue<KeyValuePair<string, Guid>>();

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
                    //Console.WriteLine("Rerunning commit thread, current number of logs = " + _nodeStorage.GetLastLogIndex() + "Match index " + JsonConvert.SerializeObject(MatchIndex));
                    while (CommitIndex < _nodeStorage.GetLastLogIndex())
                    {
                        //Console.WriteLine("Current on commit " + CommitIndex + " and last log index is " + _nodeStorage.GetLastLogIndex());

                        //Console.WriteLine("Locking Match Index 2.");
                        if (MatchIndex.Values.Count(x => x >= CommitIndex + 1) >= (_clusterOptions.MinimumNodes - 1))
                        {
                            //Console.WriteLine("Detected majority writes to logs.");
                            _stateMachine.ApplyLogToStateMachine(_nodeStorage.GetLogAtIndex(CommitIndex + 1));
                            CommitIndex++;
                        }
                        else
                        {
                            //Console.WriteLine("Majority not written...");
                        }
                        //Allow other threads to take ownership
                    }
                    Thread.Sleep(100);
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



        private Thread ShardCreationThread()
        {
            return new Thread(async () =>
            {
                Thread.CurrentThread.IsBackground = true;

                while (CurrentState == NodeState.Leader)
                {
                    DateTime startTime = DateTime.Now;
                    List<Guid> objectsToAssign = new List<Guid>();
                    string typeToAdd = null;
                    // try
                    //{
                    //Dequeue till we good
                    bool stillObjectsToAdd = true;
                    while (stillObjectsToAdd)
                    {
                        KeyValuePair<string, Guid> objectToAssign;
                        DataAssignmentQueue.TryPeek(out objectToAssign);

                        //If the type has changed, then dont proceed
                        if (typeToAdd != null && objectToAssign.Key != typeToAdd)
                        {
                            stillObjectsToAdd = false;
                        }
                        else
                        {
                            stillObjectsToAdd = DataAssignmentQueue.TryDequeue(out objectToAssign);
                        }

                        if (!stillObjectsToAdd)
                        {
                            //Logger.LogDebug("Queue has been emptied for type " + val.Key);
                            // Logger.LogError("Failed to remove item from queue.");
                        }
                        else
                        {
                            objectsToAssign.Add(objectToAssign.Value);
                            typeToAdd = objectToAssign.Key;
                        }
                    }

                    var assignedObjectPointer = 0;
                    while (assignedObjectPointer < objectsToAssign.Count())
                    {
                        ShardMetadata assignedShard;
                        if (!_stateMachine.WritableShardExists(typeToAdd, out assignedShard))
                        {
                            Logger.LogDebug("Shard created for type " + typeToAdd + " due to " + (assignedShard == null ? "no shard exists for type." : "latest shard " + assignedShard.Id + " is full."));
                            var shardNumber = assignedShard == null ? 0 : assignedShard.ShardNumber + 1;
                            Guid newShardId = Guid.NewGuid();

                            CreateShard(typeToAdd, newShardId, shardNumber);

                            while (!_stateMachine.ShardExists(newShardId))
                            {
                                Logger.LogWarning("Waiting for shard " + newShardId + " to be created...");
                            }

                            assignedShard = _stateMachine.GetShardMetaData(newShardId);
                        }

                        int currentSize = assignedShard.DataTable.Count();
                        //while (assignedObjectPointer + currentSize < assignedShard.MaxSize && assignedObjectPointer < objectsToAssign.Count())
                        //{
                        //    Logger.LogDebug("Assigning object " + objectsToAssign[assignedObjectPointer] + " to shard " + assignedShard.Id);
                        int howManyObjectsToAppend = (assignedShard.MaxSize < objectsToAssign.Count() - assignedObjectPointer) ? assignedShard.MaxSize : objectsToAssign.Count() - assignedObjectPointer;
                        DateTime currentTime = DateTime.Now;
                        var objectIds = objectsToAssign.GetRange(assignedObjectPointer, howManyObjectsToAppend).ToArray();
                        await Send(new RequestShardUpdate()
                        {
                            Action = UpdateShardAction.Append,
                            ObjectId = objectIds,
                            ShardId = assignedShard.Id
                        });
                       // Console.WriteLine("Shard update took " + (DateTime.Now - currentTime).TotalMilliseconds + " for " + objectIds.Count() + " objects.");
                        assignedObjectPointer += howManyObjectsToAppend;
                        //}
                    }
                    if (assignedObjectPointer > 0)
                        Logger.LogDebug("Assignment time took " + (DateTime.Now - startTime).TotalMilliseconds + "ms to assign " + assignedObjectPointer + " objects. Remaining in queue " + DataAssignmentQueue.Count());

                    Thread.Sleep(10);
                }
                //}
            });
        }

        #region Timeout Handlers

        //  Dictionary<string, DateTime> _monitoredShardTypes = new Dictionary<string, DateTime>();
        //   object monitoredShardsLock = new object();

        public async void ClusterInfoTimeoutHandler(object args)
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
                    await Send(new ExecuteCommands()
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

        public async Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request)
        {
            Logger.LogDebug("Detected RPC " + request.GetType().Name + ".");
            if (!IsBootstrapped)
            {
                Logger.LogDebug("Node is not ready...");
                return default(TResponse);
            }

            DateTime startCommand = DateTime.Now;
            TResponse response;
            switch (request)
            {
                case ExecuteCommands t1:
                    response = await HandleIfLeaderOrReroute(request, () => (TResponse)(object)ExecuteCommandsRPCHandler(t1));
                    break;
                case WriteData t1:
                    response = (TResponse)(object) await WriteDataRPCHandler(t1);
                    break;
                case RequestVote t1:
                    response = (TResponse)(object)RequestVoteRPCHandler(t1);
                    break;
                case AppendEntry t1:
                    response = (TResponse)(object)AppendEntryRPCHandler(t1);
                    break;
                case RequestShardUpdate t1:
                    response = (TResponse)(object)await RequestShardUpdateHandler(t1);
                    break;
                case RequestDataShard t1:
                    response = (TResponse)(object)RequestDataShardHandler(t1);
                    break;
                case AssignDataToShard t1:
                    response = await HandleIfLeaderOrReroute(request, () => (TResponse)(object)AssignDataToShardHandler(t1));
                    break;
                default:
                    throw new Exception("Request is not implemented");
            }

            if (request.RequestName == "WriteDataShard")
                Console.WriteLine("Request " + request.RequestName + " took " + (DateTime.Now - startCommand).TotalMilliseconds + "ms");
            return response;
        }

        public async Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<TResponse> Handle)
        {
            // if you change and become a leader, just handle this yourself.
            while (CurrentState != NodeState.Leader)
            {
                if (CurrentState == NodeState.Candidate)
                {
                    Logger.LogWarning("Currently a candidate during routing, will sleep thread and try again.");
                    Thread.Sleep(1000);
                }
                else
                {
                    try
                    {
                        Logger.LogDebug("Detected routing of command " + request.GetType().Name + " to leader.");
                        return (TResponse)(object)await GetLeadersConnector().Send(request);
                    }
                    catch (Exception e)
                    {
                        Logger.LogDebug("Encountered " + e.Message + " while trying to route " + request.GetType().Name + " to leader.");
                    }
                }
            }
            return Handle();
        }

        public ExecuteCommandsResponse ExecuteCommandsRPCHandler(ExecuteCommands request)
        {
            int index = _nodeStorage.AddCommands(request.Commands.ToList(), _nodeStorage.CurrentTerm);

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
                    Thread.Sleep(100);
                }
            }
            return new ExecuteCommandsResponse()
            {
                IsSuccessful = true
            };
        }

        public async Task<RequestShardUpdateResponse> RequestShardUpdateHandler(RequestShardUpdate request)
        {
            var response = await Send(new ExecuteCommands()
            {
                Commands = new List<BaseCommand>(){new UpdateShard()
                {
                    ShardId = request.ShardId,
                    Action = request.Action,
                    ObjectId = request.ObjectId
                } },
                WaitForCommits = false
            });

            return new RequestShardUpdateResponse()
            {
                IsSuccessful = response.IsSuccessful
            };
        }

        public int NumberOfWaitingThreads = 0;

        public AssignDataToShardResponse AssignDataToShardHandler(AssignDataToShard shard)
        {
            DateTime timeNow = DateTime.Now;
            var backOffMultiplier = 1;
            //If there is no queue for this type, refresh it
            // if (!DataAssignmentQueue.(shard.Type))
            // {
            //  var success = DataAssignmentQueue.TryAdd(shard.Type, new ConcurrentQueue<Guid>());
            //  if (!success)
            //  {
            //      Logger.LogError("Unable to create a new queue...");
            //  }
            // }
            DataAssignmentQueue.Enqueue(new KeyValuePair<string, Guid>(shard.Type, shard.ObjectId));

           // Console.WriteLine("Time since start " + (DateTime.Now - timeNow).TotalMilliseconds + "for checkpoint " + checkpoint++);
           // timeNow = DateTime.Now;
            Guid assignedShard;

            var processStartTime = DateTime.UtcNow;
            while (!_stateMachine.ObjectIsNewlyAssigned(shard.ObjectId, out assignedShard))
            {
                //Console.WriteLine("WAITING!!!!!");
                //Console.WriteLine("Time since start " + (DateTime.Now - timeNow).TotalMilliseconds + "for checkpoint " + checkpoint++);
                //timeNow = DateTime.Now;
                if ((DateTime.UtcNow - processStartTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                {
                    Logger.LogError("Encountered assignment timeout for data object " + shard.ObjectId);
                    Interlocked.Decrement(ref NumberOfWaitingThreads);
                    return new AssignDataToShardResponse()
                    {
                        IsSuccessful = false
                    };
                }

                Thread.Sleep(10 * backOffMultiplier);
                backOffMultiplier *= 2;
                Logger.LogDebug("Awaiting the assignment for object " + shard.ObjectId + " to become present in state.");
            }

            //Console.WriteLine("Completed assignment in " + (DateTime.Now - timeNow).TotalMilliseconds + "ms.");
            timeNow = DateTime.Now;

            return new AssignDataToShardResponse()
            {
                IsSuccessful = true,
                AssignedShard = assignedShard
            };
        }

        /// <summary>
        /// Initial update should occur on the primary node which will then replicate it to the rest of the cluster.
        /// </summary>
        /// <param name="shard"></param>
        /// <returns></returns>
        public async Task<WriteDataResponse> WriteDataRPCHandler(WriteData shard)
        {
            var newObjectGuid = Guid.NewGuid();
            // If another node has not already got an assignment for this shard
            if (shard.AssignedShard == null)
            {
                var result = await Send(new AssignDataToShard()
                {
                    ObjectId = newObjectGuid,
                    Type = shard.Type
                });

                if (!result.IsSuccessful)
                {
                    Logger.LogError("Critical error encountered when finding shard to assign data for type " + shard.Type + "...");
                    return new WriteDataResponse()
                    {
                        IsSuccessful = false
                    };
                }
                else
                {
                    shard.AssignedShard = result.AssignedShard;
                }
            }
            
            if (_stateMachine.ShardIsPrimaryOnNode(shard.AssignedShard.Value, _nodeStorage.Id))
            {
                //Send all the data to the replicas
                var saveResult = _dataRouter.WriteData(shard.Type, shard.Data, newObjectGuid);

                // Mark the shard as initialized
                await Send(new RequestShardUpdate()
                {
                    ObjectId = new Guid[] { saveResult },
                    Action = UpdateShardAction.Initialize,
                    ShardId = shard.AssignedShard.Value
                });

                //Queue up the replication tasks 

                return new WriteDataResponse()
                {
                    IsSuccessful = true
                };
            }
            else
            {
                //Reroute the initial request to the primary
                try
                {
                    // Try to send to the primary, otherwise you will have to change the primary and retry
                    NodeConnectors[_stateMachine.CurrentState.Nodes[_stateMachine.GetShardPrimaryNode(shard.AssignedShard.Value)].TransportAddress].Send(shard).GetAwaiter().GetResult();
                    return new WriteDataResponse()
                    {
                        IsSuccessful = true
                    };
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to write the shard to the primary, TODO, reassign primary and try again with new primary" + e.Message);
                    return new WriteDataResponse()
                    {
                        IsSuccessful = false
                    };
                }
            }
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
                    ConflictName = AppendEntriesExceptionNames.ConflictingLogEntryException,
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

            DateTime time = DateTime.Now;

            foreach (var log in entry.Entries)
            {
                _nodeStorage.AddLog(log);
            }

            //Console.WriteLine("Writing logs took " + (DateTime.Now - time).TotalMilliseconds);

            if (CommitIndex < entry.LeaderCommit)
            {
                Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                // Commit index will be +1 the actual index in the array
                var allLogsToBeCommited = _nodeStorage.Logs.GetRange(CommitIndex, entry.LeaderCommit - CommitIndex);
                _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                CommitIndex = allLogsToBeCommited.Last().Index;
                //Console.WriteLine("COMMITED " + CommitIndex + " with last index of logs being " + allLogsToBeCommited.Last().Index);
            }
            /*if(entry.Entries.Count() > 0)
            {
                Console.WriteLine("Added upto entry " + entry.Entries.Last().Index + " number of logs in storage " + _nodeStorage.Logs.Count());
            }*/

            return new AppendEntryResponse()
            {
                Successful = true
            };
        }

        public RequestDataShardResponse RequestDataShardHandler(RequestDataShard request)
        {
            bool found = false;
            RequestDataShardResponse data = null;

            var shardWithObject = _stateMachine.GetShardContainingObject(request.ObjectId, request.Type);

            if (shardWithObject == null)
            {
                return new RequestDataShardResponse()
                {
                    Data = null
                };
            }

            //If the data is stored here, then fetch it from here
            if (_stateMachine.NodeHasShardLatestVersion(_nodeStorage.Id, shardWithObject.Value))
            {
                return new RequestDataShardResponse()
                {
                    Data = _dataRouter.GetData(request.Type, request.ObjectId)
                };
            }
            // else route the request to all insync-nodes
            else
            {
                //Implement routing logic

                var allocatedNodes = _stateMachine.AllNodesWithUptoDateShard(shardWithObject.Value);

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
                                Logger.LogError("Failed to retrieve the data from node " + node + " for shard " + shardWithObject.Value);
                            }
                        });
            }

            while (!found)
            {
                Logger.LogDebug("Waiting for a node to respond with data for shard " + shardWithObject.Value);
                Thread.Sleep(100);
            }

            return data;
        }
        #endregion

        #region Internal Parallel Calls
        public async void SendHeartbeats()
        {
            Logger.LogDebug("Sending heartbeats");
            var tasks = NodeConnectors.Select(async connector =>
            {
                try
                {
                    var entriesToSend = new List<LogEntry>();

                    var prevLogIndex = Math.Max(0, NextIndex[connector.Key] - 1);
                    int prevLogTerm = (_nodeStorage.GetLogCount() > 0 && prevLogIndex > 0) ? prevLogTerm = _nodeStorage.GetLogAtIndex(prevLogIndex).Term : 0;

                    if (NextIndex[connector.Key] <= _nodeStorage.GetLastLogIndex() && _nodeStorage.GetLastLogIndex() != 0 && !LogsSent.GetOrAdd(connector.Key, true))
                    {
                        var unsentLogs = (_nodeStorage.GetLogCount() - NextIndex[connector.Key] + 1);
                        var quantityToSend = unsentLogs;
                        entriesToSend = _nodeStorage.Logs.GetRange(NextIndex[connector.Key] - 1, quantityToSend < maxSendEntries ? quantityToSend : maxSendEntries).ToList();
                        // entriesToSend = _nodeStorage.Logs.Where(l => l.Index >= NextIndex[connector.Key]).ToList();
                        Logger.LogDebug("Detected node " + connector.Key + " is not upto date, sending logs from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index);
                        Console.WriteLine("Sending logs with from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index + " sent " + entriesToSend.Count + "logs.");
                        LogsSent.AddOrUpdate(connector.Key, true, (key, oldvalue) =>
                        {
                            return true;
                        });
                    }

                    DateTime timeNow = DateTime.Now;
                    var result = await connector.Value.Send(new AppendEntry()
                    {
                        Term = _nodeStorage.CurrentTerm,
                        Entries = entriesToSend,
                        LeaderCommit = CommitIndex,
                        LeaderId = _nodeStorage.Id,
                        PrevLogIndex = prevLogIndex,
                        PrevLogTerm = prevLogTerm
                    });

                    LogsSent.TryUpdate(connector.Key, false, true);

                    if (result.Successful)
                    {
                        if (entriesToSend.Count() > 0)
                        {
                            var lastIndexToSend = entriesToSend.Last().Index;
                            NextIndex[connector.Key] = lastIndexToSend + 1;

                            int previousValue;
                            bool SuccessfullyGotValue = MatchIndex.TryGetValue(connector.Key, out previousValue);
                            if (!SuccessfullyGotValue)
                            {
                                Logger.LogError("Concurrency issues encountered when getting the Next Match Index");
                            }
                            var updateWorked = MatchIndex.TryUpdate(connector.Key, lastIndexToSend, previousValue);
                            //If the updated did not execute, there hs been a concurrency issue
                            while (!updateWorked)
                            {
                                //Console.WriteLine("The previous match index has been changed already, from " + previousValue);
                                SuccessfullyGotValue = MatchIndex.TryGetValue(connector.Key, out previousValue);
                                // If the match index has already exceeded the previous value, dont bother updating it
                                if (previousValue > lastIndexToSend && SuccessfullyGotValue)
                                {
                                    updateWorked = true;
                                }
                                else
                                {
                                    updateWorked = MatchIndex.TryUpdate(connector.Key, lastIndexToSend, previousValue);
                                }
                            }
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

            await Task.WhenAll(tasks);
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
                        ResetTimer(_heartbeatTimer, _clusterOptions.ElectionTimeoutMs / 12, _clusterOptions.ElectionTimeoutMs / 12);
                        StopTimer(_electionTimeoutTimer);
                        _commitThread = NewCommitThread();
                        _commitThread.Start();
                        ResetTimer(_clusterInfoTimeoutTimer, 0, 1000);
                        _shardCreationThread = ShardCreationThread(); ;
                        _shardCreationThread.Start();
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
        /// Whether there is a uncommited shard for that type of shard
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
       /* public bool UncommitedShardTypeCreation(string type)
        {
            if (_nodeStorage.Logs.GetRange(CommitIndex, _nodeStorage.GetLogCount() - CommitIndex - 1).Count(l => l.Commands.Count(bc => bc.CommandName == "CreateDataShardInformation" && ((CreateDataShardInformation)bc).Type == type) > 0) > 0)
            {
                return true;
            }
            return false;
        }*/

        /// <summary>
        /// Decide who should be the nodes storing the data
        /// </summary>
        /// <param name="type"></param>
        /// <param name="shardId"></param>
        public async void CreateShard(string type, Guid shardId, int shardNumber)
        {
            bool successfulAllocation = false;
            while (!successfulAllocation)
            {
                try
                {
                    //This is for the primary copy
                    var eligbleNodes = _stateMachine.CurrentState.Nodes;
                    var rand = new Random();
                    while (eligbleNodes.Count() == 0)
                    {
                        Logger.LogWarning("No eligible nodes found, awaiting eligible nodes.");
                        Thread.Sleep(1000);
                    }

                    var selectedNode = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count()));
                    Logger.LogDebug("Allocating data shart to node at " + selectedNode.Key);

                    await Send(new ExecuteCommands()
                    {
                        Commands = new List<CreateDataShardInformation>() {
                                new CreateDataShardInformation() {
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
                                        Initalized = false,
                                        ShardNumber = shardNumber,
                                        MaxSize = _clusterOptions.MaxShardSize
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
    }
}