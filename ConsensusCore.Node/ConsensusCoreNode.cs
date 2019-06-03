using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.RPCs;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.SystemCommands;
using ConsensusCore.Node.SystemTasks;
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
        private Thread _taskWatchThread;
        private Thread _indexCreationThread;
        private Thread _commitThread;

        private List<Thread> _taskThreads { get; set; } = new List<Thread>();

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
        ConcurrentQueue<string> IndexCreationQueue { get; set; } = new ConcurrentQueue<string>();

        /// <summary>
        /// What logs have been commited to the state
        /// </summary>
        public int CommitIndex { get; private set; }
        public int LatestLeaderCommit { get; private set; }

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

        public Dictionary<Guid, LocalShardMetaData> LocalShards { get { return _nodeStorage.ShardMetaData; } }

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

        public Thread GetCommitThread()
        {
            return new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                while (CurrentState == NodeState.Leader || CurrentState == NodeState.Follower)
                {
                    //As leader, calculate the commit index
                    if (CurrentState == NodeState.Leader)
                    {
                        var indexToAddTo = _nodeStorage.GetLastLogIndex();
                        while (CommitIndex < indexToAddTo)
                        {
                            if (MatchIndex.Values.Count(x => x >= indexToAddTo) >= (_clusterOptions.MinimumNodes - 1))
                            {
                                _stateMachine.ApplyLogsToStateMachine(_nodeStorage.Logs.GetRange(CommitIndex, indexToAddTo - CommitIndex));// _nodeStorage.GetLogAtIndex(CommitIndex + 1));
                                CommitIndex = indexToAddTo;
                                Thread.Sleep(100);
                            }
                            else
                            {
                            }
                            indexToAddTo--;
                        }
                    }
                    else if (CurrentState == NodeState.Follower)
                    {
                        if (CommitIndex < LatestLeaderCommit)
                        {
                            //Console.WriteLine("Commiting indexes from " + CommitIndex + " to " + LatestLeaderCommit);
                            var allLogsToBeCommited = _nodeStorage.Logs.GetRange(CommitIndex, LatestLeaderCommit - CommitIndex);
                            _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                            CommitIndex = allLogsToBeCommited.Last().Index;
                        }
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

        private Thread GetIndexCreationThread()
        {
            return new Thread(async () =>
            {
                while (CurrentState == NodeState.Leader)
                {
                    string typeToCreate;
                    bool isSuccessful;
                    do
                    {
                        isSuccessful = IndexCreationQueue.TryDequeue(out typeToCreate);
                        if (isSuccessful)
                        {
                            if (!_stateMachine.IndexExists(typeToCreate))
                            {
                                CreateIndex(typeToCreate);
                            }
                            else
                            {
                                Console.WriteLine("INDEX Already exists");
                            }
                        }
                    }
                    while (isSuccessful);

                    Thread.Sleep(1000);
                }
            });
        }

        /*private Thread ShardUpdateThread()
        {
            return new Thread(async () =>
            {
                Thread.CurrentThread.IsBackground = true;

                while (CurrentState == NodeState.Leader)
                {
                    DateTime startTime = DateTime.Now;
                    List<ShardDataUpdate> objectsToAssign = new List<ShardDataUpdate>();
                    string typeToAdd = null;

                    bool stillObjectsToAdd = true;
                    while (stillObjectsToAdd)
                    {
                        KeyValuePair<string, ShardDataUpdate> objectToAssign;
                        DataAssignmentQueue.TryPeek(out objectToAssign);

                        //If the type has changed, then dont proceed
                        if ((typeToAdd != null && objectToAssign.Key != typeToAdd)  || objectsToAssign.Count() > 100)
                        {
                            //stillObjectsToAdd = false;
                            break;
                        }
                        else
                        {
                            stillObjectsToAdd = DataAssignmentQueue.TryDequeue(out objectToAssign);
                        }

                        if (!stillObjectsToAdd)
                        {
                        }
                        else
                        {
                            objectsToAssign.Add(objectToAssign.Value);
                            typeToAdd = objectToAssign.Key;
                        }
                    }

                    var assignedObjectPointer = 0;
                    Dictionary<Guid, ShardDataUpdate> objectUpdates = new Dictionary<Guid, ShardDataUpdate>();
                    while (assignedObjectPointer < objectsToAssign.Count())
                    {

                        if (objectsToAssign[assignedObjectPointer].Action == UpdateShardAction.Append)
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

                            objectUpdates.Add(objectsToAssign[assignedObjectPointer].DataId, new ShardDataUpdate()
                            {
                                Action = objectsToAssign[assignedObjectPointer].Action,
                                DataId = objectsToAssign[assignedObjectPointer].DataId,
                                ShardId = assignedShard.Id
                            });
                        }
                        else
                        {
                            objectUpdates.Add(objectsToAssign[assignedObjectPointer].DataId, new ShardDataUpdate()
                            {
                                Action = objectsToAssign[assignedObjectPointer].Action,
                                DataId = objectsToAssign[assignedObjectPointer].DataId,
                                ShardId = objectsToAssign[assignedObjectPointer].ShardId
                            });
                        }

                        assignedObjectPointer++;
                        //}
                    }

                    if (objectUpdates.Count() > 0)
                        await Send(new RequestShardUpdate()
                        {
                            Updates = objectUpdates
                        });
                    // Console.WriteLine("Shard update took " + (DateTime.Now - currentTime).TotalMilliseconds + " for " + objectIds.Count() + " objects.");
                    assignedObjectPointer += objectUpdates.Count();

                    if (assignedObjectPointer > 0)
                        Logger.LogDebug("Assignment time took " + (DateTime.Now - startTime).TotalMilliseconds + "ms to assign " + assignedObjectPointer + " objects. Remaining in queue " + DataAssignmentQueue.Count());

                    //Thread.Sleep(100);
                }
                //}
            });
        }*/

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
                foreach (var url in _clusterOptions.NodeUrls)//.Where(url => url != MyUrl && _stateMachine.CurrentState.Nodes.ContainsKey(_nodeStorage.Id)))
                {
                    Guid? nodeId = null;
                    try
                    {
                        var tempConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs));
                        nodeId = (await tempConnector.GetNodeInfoAsync()).Id;

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

        public Thread GetTaskWatchThread()
        {
            return new Thread(() =>
            {
                while (CurrentState == NodeState.Follower || CurrentState == NodeState.Leader)
                {
                    if (IsBootstrapped)
                    {
                        // Console.WriteLine("Checking tasks...");
                        //Check tasks assigned to this node
                        var tasks = _stateMachine.CurrentState.ClusterTasks.Where(t => t.Status == ClusterTaskStatuses.Created && t.NodeId == _nodeStorage.Id).ToArray();
                        var currentTasksNo = _taskThreads.Where(t => t.IsAlive).Count();
                        var numberOfTasksToAssign = (tasks.Count() > (_clusterOptions.ConcurrentTasks - currentTasksNo)) ? (_clusterOptions.ConcurrentTasks - currentTasksNo) : tasks.Count();
                        //Create a thread for each task
                        for (var i = 0; i < numberOfTasksToAssign; i++)
                        {
                            Thread newThread = GetTaskThread(tasks[i]);
                            newThread.Start();
                            _taskThreads.Add(newThread);
                        }
                    }
                    Thread.Sleep(1000);
                }
            });
        }

        public Thread GetTaskThread(BaseTask task)
        {
            return new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                switch (task)
                {
                    case ReplicateShard t:
                        Console.WriteLine("Detected replication of shard tasks");
                        break;
                }
            });
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
                    response = (TResponse)(object)await WriteDataRPCHandler(t1);
                    break;
                case RequestVote t1:
                    response = (TResponse)(object)RequestVoteRPCHandler(t1);
                    break;
                case AppendEntry t1:
                    response = (TResponse)(object)AppendEntryRPCHandler(t1);
                    break;
                case RequestDataShard t1:
                    response = (TResponse)(object)RequestDataShardHandler(t1);
                    break;
                case RequestClusterTasksUpsert t1:
                    response = await HandleIfLeaderOrReroute(request, () => (TResponse)(object)(RequestClusterTasksUpsertHandler(t1)).GetAwaiter().GetResult());
                    break;
                case RequestCreateIndex t1:
                    response = await HandleIfLeaderOrReroute(request, () => (TResponse)(object)(CreateIndexHandler(t1)));
                    break;
                case RequestInitializeNewShard t1:
                    response = (TResponse)(object)RequestInitializeNewShardHandler(t1);
                    break;
                case ReplicateShardOperation t1:
                    response = (TResponse)(object)ReplicateShardOperationHandler(t1);
                    break;
                default:
                    throw new Exception("Request is not implemented");
            }

            // if (request.RequestName == "WriteDataShard")
            //    Console.WriteLine("Request " + request.RequestName + " took " + (DateTime.Now - startCommand).TotalMilliseconds + "ms");
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

        public ReplicateShardOperationResponse ReplicateShardOperationHandler(ReplicateShardOperation request)
        {
            try
            {
                _nodeStorage.ReplicateShardOperation(request.ShardId, request.Pos, request.Operation);
                _dataRouter.WriteData(request.Payload);
                return new ReplicateShardOperationResponse()
                {
                    IsSuccessful = true
                };
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to replicated data " + request.Payload.Id + " with exception " + e.StackTrace);
                return new ReplicateShardOperationResponse()
                {
                    IsSuccessful = false
                };
            }
        }

        public RequestCreateIndexResponse CreateIndexHandler(RequestCreateIndex request)
        {
            IndexCreationQueue.Enqueue(request.Type);
            return new RequestCreateIndexResponse()
            {
                IsSuccessful = true
            };
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

        public RequestInitializeNewShardResponse RequestInitializeNewShardHandler(RequestInitializeNewShard request)
        {
            _nodeStorage.AddNewShardMetaData(new LocalShardMetaData()
            {
                ShardId = request.ShardId,
                ShardOperations = new SortedList<int, ShardOperation>(),
                Type = request.Type
            });

            return new RequestInitializeNewShardResponse()
            {
            };
        }

        /*public async Task<RequestShardUpdateResponse> RequestShardUpdateHandler(RequestShardUpdate request)
        {
            var response = await Send(new ExecuteCommands()
            {
                Commands = new List<BaseCommand>(){new UpdateShards()
                {
                    Updates = request.Updates
                } },
                WaitForCommits = false
            });

            return new RequestShardUpdateResponse()
            {
                IsSuccessful = response.IsSuccessful
            };
        }*/

        /* public int NumberOfWaitingThreads = 0;

         public AssignDataToShardResponse AssignDataToShardHandler(AssignDataToShard shard)
         {
             DateTime timeNow = DateTime.Now;
             var backOffMultiplier = 1;


             //  Console.WriteLine("Creating object with id " + shard.ObjectId);

             DataAssignmentQueue.Enqueue(new KeyValuePair<string, ShardDataUpdate>(shard.Type, new ShardDataUpdate()
             {
                 Action = UpdateShardAction.Append,
                 DataId = shard.ObjectId
             }));
             Guid assignedShard;

             var processStartTime = DateTime.UtcNow;

             while (!_stateMachine.ObjectIsNewlyAssigned(shard.ObjectId, out assignedShard))
             {
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
         }*/

        public async Task<RequestClusterTasksUpsertResponse> RequestClusterTasksUpsertHandler(RequestClusterTasksUpsert request)
        {
            var result = await Send(new ExecuteCommands()
            {
                Commands = new List<BaseCommand>() { new UpsertClusterTasks()
                {
                    ClusterTasks = request.Tasks
                } },
                WaitForCommits = true
            });

            return new RequestClusterTasksUpsertResponse()
            {
                IsSuccessful = result.IsSuccessful
            };
        }

        Dictionary<Guid, object> Locks = new Dictionary<Guid, object>();

        /// <summary>
        /// Initial update should occur on the primary node which will then replicate it to the rest of the cluster.
        /// </summary>
        /// <param name="shard"></param>
        /// <returns></returns>
        public async Task<WriteDataResponse> WriteDataRPCHandler(WriteData shard)
        {
            //Check if index exists, if not - create one
            if (!_stateMachine.IndexExists(shard.Data.Type))
            {
                await Send(new RequestCreateIndex()
                {
                    Type = shard.Data.Type
                });

                while (!_stateMachine.IndexExists(shard.Data.Type))
                {
                    Thread.Sleep(100);
                }
            }

            SharedShardMetadata shardMetadata;

            if (shard.Data.ShardId == null)
            {
                var allocations = _stateMachine.GetShards(shard.Data.Type);
                Random rand = new Random();
                var selectedNodeIndex = rand.Next(0, allocations.Length);
                shard.Data.ShardId = allocations[selectedNodeIndex].Id;
                shardMetadata = allocations[selectedNodeIndex];
            }
            else
            {
                shardMetadata = _stateMachine.GetShard(shard.Data.Type, shard.Data.ShardId.Value);
            }

            //If the shard is assigned to you
            if (shardMetadata.PrimaryAllocation == _nodeStorage.Id)
            {
                var sequenceNumber = _nodeStorage.AddNewShardOperation(shard.Data.ShardId.Value, new ShardOperation()
                {
                    ObjectId = shard.Data.Id,
                    Operation = ShardOperationOptions.Create
                });
                _dataRouter.WriteData(shard.Data);

                //All allocations except for your own
                foreach (var allocation in shardMetadata.InsyncAllocations.Where(id => id != _nodeStorage.Id))
                {
                    var result = NodeConnectors[_stateMachine.CurrentState.Nodes[allocation].TransportAddress].Send(new ReplicateShardOperation()
                    {
                        ShardId = shardMetadata.Id,
                        Operation = new ShardOperation()
                        {
                            ObjectId = shard.Data.Id,
                            Operation = ShardOperationOptions.Create
                        },
                        Payload = shard.Data,
                        Pos = sequenceNumber
                    }).GetAwaiter().GetResult();

                    if (result.IsSuccessful)
                    {
                        Console.WriteLine("Successfully replicated all shards.");

                    }
                    else
                    {
                        Logger.LogError("Failed to replicate shard, marking shard as not insync...");
                    }
                }

                return new WriteDataResponse()
                {
                    IsSuccessful = true
                };
            }
            else
            {
                NodeConnectors[_stateMachine.CurrentState.Nodes[shardMetadata.PrimaryAllocation].TransportAddress].Send(shard).GetAwaiter().GetResult();
                return new WriteDataResponse()
                {
                    IsSuccessful = true
                };
            }

            return new WriteDataResponse()
            {
                IsSuccessful = true
            };
            //Decide which shard to write to

            //

            /*
            var now = DateTime.Now;
            if (shard.AssignedGuid == null)
            {
                shard.AssignedGuid = Guid.NewGuid();
            }
            // If another node has not already got an assignment for this shard
            if (shard.AssignedShard == null)
            {
                //Console.WriteLine("Assigning data");
                var result = await Send(new AssignDataToShard()
                {
                    ObjectId = shard.AssignedGuid.Value,
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
                Console.WriteLine("Assignment took " + (DateTime.Now - now).TotalMilliseconds);
            }
            now = DateTime.Now;
            if (_stateMachine.ShardIsPrimaryOnNode(shard.AssignedShard.Value, _nodeStorage.Id))
            {
                //Send all the data to the replicas
                var saveResult = _dataRouter.WriteData(shard.Type, shard.Data, shard.AssignedGuid.Value);

                var allocations = new Dictionary<Guid, int>(_stateMachine.GetShardMetaData(shard.AssignedShard.Value).Allocations);

                // Mark the shard as initialized
                DataAssignmentQueue.Enqueue(new KeyValuePair<string, ShardDataUpdate>(shard.Type, new ShardDataUpdate()
                {
                    Action = UpdateShardAction.Initialize,
                    DataId = shard.AssignedGuid.Value,
                    ShardId = shard.AssignedShard.Value
                }));

                Console.WriteLine("Primary write took " + (DateTime.Now - now).TotalMilliseconds);

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
            }*/
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

            if (entry.LeaderCommit > LatestLeaderCommit)
            {
                Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                // Commit index will be +1 the actual index in the array
                /*var allLogsToBeCommited = _nodeStorage.Logs.GetRange(CommitIndex, entry.LeaderCommit - CommitIndex);
                _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                CommitIndex = allLogsToBeCommited.Last().Index;*/
                LatestLeaderCommit = entry.LeaderCommit;
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

            /* var shardWithObject = _stateMachine.GetShardContainingObject(request.ObjectId, request.Type);

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
             }*/

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
                        // Console.WriteLine("Sending logs with from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index + " sent " + entriesToSend.Count + "logs.");
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
            if (newState == NodeState.Candidate && !_nodeOptions.EnableLeader)
            {
                Logger.LogWarning("Tried to promote to candidate but node has been disabled.");
                return;
            }

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
                        RestartThread(ref _taskWatchThread, () => GetTaskWatchThread());
                        RestartThread(ref _commitThread, () => GetCommitThread());
                        break;
                    case NodeState.Leader:
                        CurrentLeader = new KeyValuePair<Guid?, string>(_nodeStorage.Id, MyUrl);
                        ResetLeaderState();
                        ResetTimer(_heartbeatTimer, _clusterOptions.ElectionTimeoutMs / 12, _clusterOptions.ElectionTimeoutMs / 12);
                        ResetTimer(_clusterInfoTimeoutTimer, 0, 1000);
                        StopTimer(_electionTimeoutTimer);
                        RestartThread(ref _commitThread, () => GetCommitThread());
                        RestartThread(ref _indexCreationThread, () => GetIndexCreationThread());
                        RestartThread(ref _taskWatchThread, () => GetTaskWatchThread());
                        break;
                }
            }
        }

        private Thread RestartThread(ref Thread thread, Func<Thread> threadFunction)
        {
            if (thread == null || !thread.IsAlive)
            {
                thread = threadFunction();
                thread.Start();
            }
            return thread;
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
        public async void CreateIndex(string type)
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

                    List<SharedShardMetadata> Shards = new List<SharedShardMetadata>();

                    for (var i = 0; i < _clusterOptions.NumberOfShards; i++)
                    {
                        Shards.Add(new SharedShardMetadata()
                        {
                            InsyncAllocations = eligbleNodes.Keys.ToList(),//new List<Guid> { selectedNode.Key },
                            PrimaryAllocation = selectedNode.Key,
                            Id = Guid.NewGuid()
                        });

                        foreach (var allocationI in Shards[i].InsyncAllocations)
                        {
                            if (allocationI != _nodeStorage.Id)
                            {
                                NodeConnectors[_stateMachine.CurrentState.Nodes[allocationI].TransportAddress].Send(new RequestInitializeNewShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                }).GetAwaiter().GetResult();
                            }
                            else
                            {
                                Send(new RequestInitializeNewShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                }).GetAwaiter().GetResult();
                            }
                        }
                    }

                    var result = await Send(new ExecuteCommands()
                    {
                        Commands = new List<CreateIndex>() {
                                new CreateIndex() {
                                        Type = type,
                                        Shards = Shards
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