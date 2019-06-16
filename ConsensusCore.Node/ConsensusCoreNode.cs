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
        //private Timer _electionTimeoutTimer;
        private Timer _clusterInfoTimeoutTimer;
        private Thread _taskWatchThread;
        private Thread _indexCreationThread;
        private Thread _commitThread;
        private Thread _bootstrapThread;
        private Thread _nodeSelfHealingThread;
        private Thread _shardReassignmentThread;
        private Thread _electionThread;

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
        public StateMachine<State> _stateMachine { get; private set; }
        public string MyUrl { get; private set; }
        public bool IsBootstrapped = false;
        public KeyValuePair<Guid?, string> CurrentLeader;
        private Thread _findLeaderThread;
        private int maxSendEntries = 10000;
        public IDataRouter _dataRouter;
        public bool enableDataRouting = false;
        public bool InCluster { get; private set; } = false;
        ConcurrentQueue<string> IndexCreationQueue { get; set; } = new ConcurrentQueue<string>();
        public bool CompletedFirstLeaderDiscovery = false;

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

            if (!_clusterOptions.TestMode)
            {
                _bootstrapThread = new Thread(() =>
                {
                    //Wait for the rest of the node to bootup
                    Thread.Sleep(3000);
                    BootstrapNode().GetAwaiter().GetResult();
                });

                _bootstrapThread.Start();
            }
            else
            {
                Console.WriteLine("Running in test mode...");
                IsBootstrapped = true;
            }
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
                            var numberOfLogs = _nodeStorage.Logs.Count(); ;
                            //On resync, the commit index could be higher then the local amount of logs available
                            var commitIndexToSyncTill = numberOfLogs < LatestLeaderCommit ? numberOfLogs : LatestLeaderCommit;
                            var allLogsToBeCommited = _nodeStorage.Logs.GetRange(CommitIndex, commitIndexToSyncTill - CommitIndex);
                            if (allLogsToBeCommited.Count > 0)
                            {
                                _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                                CommitIndex = allLogsToBeCommited.Last().Index;
                            }
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
                    NodeInformation node;
                    if ((node = _stateMachine.GetNode(id)) == null)
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

        int syncThreads = 0;
        public Thread NodeSelfHealingThread()
        {
            return new Thread(async () =>
            {
                Thread.CurrentThread.IsBackground = true;
                while (CurrentState == NodeState.Follower || CurrentState == NodeState.Leader)
                {
                    if (InCluster)
                    {
                        Logger.LogDebug("Starting self healing thread.");
                        //This will get you all the out of sync shards
                        var outOfSyncShards = _stateMachine.GetAllOutOfSyncShards(_nodeStorage.Id);

                        if (outOfSyncShards.Count() > 0)
                        {
                            //foreach out of sync, resync the threads
                            var recoveryTasks = outOfSyncShards.Select(async shard =>
                            {
                                try
                                {
                                    Interlocked.Increment(ref syncThreads);
                                    Logger.LogInformation("Start sync. Current threads being synced " + syncThreads);
                                    await SyncShard(shard.Id, shard.Type);
                                    Logger.LogInformation("Resynced " + shard.Id + " successfully.");
                                }
                                catch (Exception e)
                                {
                                    Logger.LogError("Failed to sync shard " + shard.Id + " with error " + e.StackTrace);
                                }
                                Interlocked.Decrement(ref syncThreads);
                            });

                            await Task.WhenAll(recoveryTasks);

                            Logger.LogInformation("Resynced all shards successfully.");
                        }
                    }
                    Thread.Sleep(1000);
                }
            });
        }

        public async Task<bool> BootstrapNode()
        {
            Logger.LogInformation("Bootstrapping Node!");

            // The node cannot bootstrap unless at least a majority of nodes are present

            while (NodeConnectors.Count() < _clusterOptions.MinimumNodes - 1)
            {
                NodeConnectors.Clear();
                foreach (var url in _clusterOptions.NodeUrls)
                {
                    var testConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs));
                    //Always add the connector
                    NodeConnectors.Add(url, testConnector);
                    Guid? nodeId = null;
                    try
                    {
                        nodeId = (await testConnector.GetNodeInfoAsync()).Id;
                        if (nodeId == _nodeStorage.Id)
                        {
                            MyUrl = url;
                            NodeConnectors.Remove(url);
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
            return true;
        }

        private Thread GetShardReassignmentThread()
        {
            return new Thread(async () =>
            {
                while (CurrentState == NodeState.Leader)
                {
                    foreach (var shard in _stateMachine.GetShards())
                    {
                        //Add the node to stale shards to start syncing
                        List<Guid> newStaleAllocations = new List<Guid>();
                        foreach (var node in _stateMachine.GetNodes())
                        {
                            //It is not stale or insync
                            if (!shard.InsyncAllocations.Contains(node.Id) && !shard.StaleAllocations.Contains(node.Id))
                            {
                                newStaleAllocations.Add(node.Id);
                            }
                        }
                        if (newStaleAllocations.Count() > 0)
                        {
                            var shardInfo = _stateMachine.GetShard(shard.Type, shard.Id);
                            HashSet<Guid> staleNodes = shardInfo.StaleAllocations.ToHashSet<Guid>();
                            foreach (var newStaleNode in newStaleAllocations)
                                staleNodes.Add(newStaleNode);

                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>() {
                                new UpdateShardMetadata()
                                {
                                    StaleAllocations = newStaleAllocations,
                                    ShardId = shard.Id,
                                    InsyncAllocations = shardInfo.InsyncAllocations,
                                    PrimaryAllocation = shardInfo.PrimaryAllocation,
                                    Type = shardInfo.Type
                                }
                            }
                            });

                            Logger.LogInformation("Added " + newStaleAllocations.Count() + " nodes to " + shard.Id);
                        }
                    }

                    Thread.Sleep(3000);
                }
            });
        }

        private Thread GetIndexCreationThread()
        {
            return new Thread(() =>
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

        #region Timeout Handlers



        public async void ClusterInfoTimeoutHandler(object args)
        {
            var nodeUpsertCommands = new List<BaseCommand>();
            if (!_clusterOptions.TestMode)
            {
                Logger.LogDebug("Rediscovering nodes...");
                List<string> UncontactableUrl = new List<string>();
                foreach (var url in _clusterOptions.NodeUrls)
                {
                    try
                    {
                        Guid? nodeId = (await new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs)).GetNodeInfoAsync()).Id;

                        var possibleNodeUpdate = new NodeInformation()
                        {
                            Name = "",
                            TransportAddress = url
                        };

                        //If the node does not exist
                        if (nodeId.Value != null && (!_stateMachine.CurrentState.Nodes.ContainsKey(nodeId.Value) ||
                            // Check whether the node with the same id has different information
                            !_stateMachine.CurrentState.Nodes[nodeId.Value].Equals(possibleNodeUpdate)
                            //Node was uncontactable now its contactable
                            || !_stateMachine.IsNodeContactable(nodeId.Value)
                            ))
                        {
                            Logger.LogDebug("Detected updated for node " + nodeId);
                            nodeUpsertCommands.Add((BaseCommand)new UpsertNodeInformation()
                            {
                                Id = nodeId.Value,
                                Name = "",
                                TransportAddress = url,
                                IsContactable = true
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

                            /* if(!NodeConnectors.ContainsKey(url))
                             {
                                 NodeConnectors.Add(url, new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs)));
                             }*/
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Node at url " + url + " was unreachable...");
                        UncontactableUrl.Add(url);
                    }
                }
                var rand = new Random();
                foreach (var url in UncontactableUrl)
                {
                    Logger.LogWarning("Node at " + url + " is uncontactable, reassigning all nodes");
                    var node = _stateMachine.GetNode(url);
                    if (node != null)
                    {
                        var nodeId = node.Id;
                        foreach (var shard in _stateMachine.GetAllPrimaryShards(nodeId))
                        {
                            Console.WriteLine("Reassigned shard " + shard.Id);
                            nodeUpsertCommands.Add(new UpdateShardMetadata()
                            {
                                PrimaryAllocation = shard.InsyncAllocations.Where(s => s != nodeId).ElementAt(rand.Next(0, shard.InsyncAllocations.Count - 1)),
                                ShardId = shard.Id,
                                Type = shard.Type
                            });
                        }

                        if (_stateMachine.IsNodeContactable(nodeId))
                        {
                            nodeUpsertCommands.Add(new UpsertNodeInformation()
                            {
                                IsContactable = false,
                                Id = nodeId,
                                TransportAddress = node.TransportAddress
                            });
                        }
                    }
                }

            }
            else if (!CompletedFirstLeaderDiscovery)
            {
                //In the test, just add your own node
                nodeUpsertCommands.Add((BaseCommand)new UpsertNodeInformation()
                {
                    Id = _nodeStorage.Id,
                    Name = "",
                    IsContactable = true
                });
            }

            if (CurrentState == NodeState.Leader)
                if (nodeUpsertCommands.Count > 0)
                {
                    await Send(new ExecuteCommands()
                    {
                        Commands = nodeUpsertCommands,
                        WaitForCommits = true
                    });
                }
            CompletedFirstLeaderDiscovery = true;
        }

        public Thread GetTaskWatchThread()
        {
            return new Thread(() =>
            {
                while (CurrentState == NodeState.Follower || CurrentState == NodeState.Leader)
                {
                    if (IsBootstrapped)
                    {
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
            return new Thread(async () =>
            {
                Thread.CurrentThread.IsBackground = true;
                switch (task)
                {
                    case ReplicateShard t:
                        Console.WriteLine("Detected replication of shard tasks");
                        break;
                    case ResyncShard t:/*
                        var currentPos = _nodeStorage.GetCurrentShardPos(t.ShardId);
                        var shardMetadata = _stateMachine.GetShard(t.Type, t.ShardId);
                        var lastPosition = 0;
                        do
                        {
                            var result = await Send(new RequestShardOperations()
                            {
                                OperationPos = currentPos + 1,
                                ShardId = t.ShardId,
                                Type = t.Type
                            });

                            if (result.IsSuccessful)
                            {
                                var replicationResult = await Send(new ReplicateShardOperation()
                                {
                                    Operation = new ShardOperation()
                                    {
                                        ObjectId = result.ObjectId,
                                        Operation = result.Operation
                                    },
                                    Payload = result.Payload,
                                    Pos = result.Pos,
                                    ShardId = t.ShardId
                                });

                                if (replicationResult.IsSuccessful)
                                {
                                    Console.WriteLine("Replicated upto " + currentPos);
                                    lastPosition = result.LatestOperationNo;
                                }
                                currentPos++;
                            }
                        }
                        while (currentPos < lastPosition);*/

                        break;
                }
            });
        }

        public async void ElectionTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                var random = new Random();
                SetNodeRole(NodeState.Candidate);
                //Wait a random amount of time
                Thread.Sleep(random.Next(0, _clusterOptions.ElectionTimeoutMs / 2));
                //Reset the timer to prevent overlap
                ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                Logger.LogDebug("Detected election timeout event.");

                var totalVotes = 1;

                var tasks = NodeConnectors.Select(async connector =>
                {
                    try
                    {
                        var result = await connector.Value.Send(new RequestVote()
                        {
                            Term = _nodeStorage.CurrentTerm,
                            CandidateId = _nodeStorage.Id,
                            LastLogIndex = _nodeStorage.GetLastLogIndex(),
                            LastLogTerm = _nodeStorage.GetLastLogTerm()
                        });

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

                await Task.WhenAll(tasks);

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
            try
            {

                Logger.LogDebug("Detected RPC " + request.GetType().Name + ".");
                if (!IsBootstrapped)
                {
                    Logger.LogDebug("Node is not ready...");
                    return default(TResponse);
                }

                if (IsClusterRequest<TResponse>(request) && !InCluster)
                {
                    Logger.LogWarning("Reqeuest rejected, node is not apart of cluster...");
                    return default(TResponse);
                    //throw new Exception("Not apart of cluster yet...");
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
                        response = (TResponse)(object)await RequestDataShardHandler(t1);
                        break;
                    case RequestClusterTasksUpsert t1:
                        response = await HandleIfLeaderOrReroute(request, () => (TResponse)(object)(RequestClusterTasksUpsertHandler(t1)));
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
                    case RequestShardOperations t1:
                        response = (TResponse)(object)RequestShardOperationsHandler(t1);
                        break;
                    default:
                        throw new Exception("Request is not implemented");
                }
                return response;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public bool IsClusterRequest<TResponse>(IClusterRequest<TResponse> request)
        {
            switch (request)
            {
                case RequestDataShard t1:
                    return true;
                case RequestClusterTasksUpsert t1:
                    return true;
                case RequestCreateIndex t1:
                    return true;
                case RequestInitializeNewShard t1:
                    return true;
                case ReplicateShardOperation t1:
                    return true;
                default:
                    return false;
            }
        }

        public RequestShardOperationsResponse RequestShardOperationsHandler(RequestShardOperations request)
        {
            //Check that the shard is insync here
            var localShard = _nodeStorage.GetShardMetadata(request.ShardId);

            if (localShard == null)
            {
                Logger.LogError("Request for shard " + request.ShardId + " however shard does not exist on node.");
                return new RequestShardOperationsResponse()
                {
                    IsSuccessful = false
                };
            }

            SortedDictionary<int, ShardOperationMessage> FinalList = new SortedDictionary<int, ShardOperationMessage>();

            //foreach value in from - to, pull the operation and then pull the object from object router
            for (var i = request.From; i <= request.To; i++)
            {
                var operation = _nodeStorage.GetOperation(request.ShardId, i);
                if (operation != null)
                {
                    //This data could be in a future state
                    var currentData = (ShardData)_dataRouter.GetData(request.Type, operation.ObjectId);
                    FinalList.Add(i, new ShardOperationMessage()
                    {
                        ObjectId = operation.ObjectId,
                        Operation = operation.Operation,
                        Payload = currentData,
                        Position = i
                    });
                }
            }

            return new RequestShardOperationsResponse()
            {
                IsSuccessful = true,
                LatestPosition = _nodeStorage.GetCurrentShardPos(request.ShardId),
                Operations = FinalList
            };
        }

        public async Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<TResponse> Handle)
        {
            var CurrentTime = DateTime.Now;
            // if you change and become a leader, just handle this yourself.
            while (CurrentState != NodeState.Leader)
            {
                if (CurrentState == NodeState.Candidate)
                {
                    if ((DateTime.Now - CurrentTime).TotalMilliseconds < _clusterOptions.LatencyToleranceMs)
                    {
                        Logger.LogWarning("Currently a candidate during routing, will sleep thread and try again.");
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        return default(TResponse);
                    }
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
                if (request.Operation.Operation == ShardOperationOptions.Update || request.Operation.Operation == ShardOperationOptions.Delete)
                {
                    while ((request.Pos - 1) > LocalShards[request.ShardId].SyncPos)
                    {
                        Logger.LogDebug("Waiting for log to sync up with position " + (request.Pos - 1));
                        Thread.Sleep(30);
                    }
                }

                var applyOperation = _nodeStorage.ReplicateShardOperation(request.ShardId, request.Pos, request.Operation);

                if (applyOperation)
                {
                    switch (request.Operation.Operation)
                    {
                        case ShardOperationOptions.Create:
                            _dataRouter.InsertData(request.Payload);
                            break;
                        case ShardOperationOptions.Update:
                            _dataRouter.UpdateData(request.Payload);
                            break;
                    }
                }
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
                ShardOperations = new ConcurrentDictionary<int, ShardOperation>(),
                Type = request.Type
            });

            return new RequestInitializeNewShardResponse()
            {
            };
        }

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
        /// Note, the request will fail if the primary goes down during the request
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
                    Operation = shard.Operation
                });


                switch (shard.Operation)
                {
                    case ShardOperationOptions.Create:
                        _dataRouter.InsertData(shard.Data);
                        break;
                    case ShardOperationOptions.Update:
                        _dataRouter.UpdateData(shard.Data);
                        break;
                }

                List<Guid> InvalidNodes = new List<Guid>();
                //All allocations except for your own
                foreach (var allocation in shardMetadata.InsyncAllocations.Where(id => id != _nodeStorage.Id))
                {
                    try
                    {
                        var result = await NodeConnectors[_stateMachine.CurrentState.Nodes[allocation].TransportAddress].Send(new ReplicateShardOperation()
                        {
                            ShardId = shardMetadata.Id,
                            Operation = new ShardOperation()
                            {
                                ObjectId = shard.Data.Id,
                                Operation = shard.Operation
                            },
                            Payload = shard.Data,
                            Pos = sequenceNumber
                        });

                        if (result.IsSuccessful)
                        {
                            Logger.LogDebug("Successfully replicated all " + shardMetadata.Id + "shards.");
                        }
                        else
                        {
                            throw new Exception("Failed to replicate data to shard " + shardMetadata.Id + " to node " + allocation);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogError("Failed to replicate shard, marking shard as not insync...");
                        InvalidNodes.Add(allocation);
                    }
                }


                if (InvalidNodes.Count() > 0)
                {
                    HashSet<Guid> allStaleAllocations = shardMetadata.StaleAllocations.ToHashSet();
                    foreach (var node in InvalidNodes)
                        allStaleAllocations.Add(node);
                    Logger.LogInformation("Detected invalid nodes, setting nodes to be out-of-sync");

                    await Send(new ExecuteCommands()
                    {
                        Commands = new List<BaseCommand>()
                        {
                            new UpdateShardMetadata()
                            {
                                ShardId = shardMetadata.Id,
                                PrimaryAllocation = shardMetadata.PrimaryAllocation,
                                Type = shardMetadata.Type,
                                InsyncAllocations = shardMetadata.InsyncAllocations.Where(a => !InvalidNodes.Contains(a)).ToList(),
                                StaleAllocations = allStaleAllocations.ToList()
                            }
                        }
                    });
                }

                return new WriteDataResponse()
                {
                    IsSuccessful = true,
                    ShardId = shard.Data.ShardId.Value
                };
            }
            else
            {
                try
                {
                    await NodeConnectors[_stateMachine.CurrentState.Nodes[shardMetadata.PrimaryAllocation].TransportAddress].Send(shard);
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to forward request to node " + _stateMachine.CurrentState.Nodes[shardMetadata.PrimaryAllocation].TransportAddress);
                }
                return new WriteDataResponse()
                {
                    IsSuccessful = true,
                    ShardId = shard.Data.ShardId.Value
                };
            }
        }

        public RequestVoteResponse RequestVoteRPCHandler(RequestVote requestVoteRPC)
        {
            var successful = false;
            if (IsBootstrapped)
            {
                //Ref1 $5.2, $5.4
                if (_nodeStorage.CurrentTerm <= requestVoteRPC.Term && ((_nodeStorage.VotedFor == null || _nodeStorage.VotedFor == requestVoteRPC.CandidateId) &&
                (requestVoteRPC.LastLogIndex >= _nodeStorage.GetLogCount() && requestVoteRPC.LastLogTerm >= _nodeStorage.GetLastLogTerm())))
                {
                    _nodeStorage.SetVotedFor(requestVoteRPC.CandidateId);
                    Logger.LogInformation("Voting for " + requestVoteRPC.CandidateId + " for term " + requestVoteRPC.Term);
                    SetNodeRole(NodeState.Follower);
                    successful = true;
                }
                else if (_nodeStorage.CurrentTerm > requestVoteRPC.Term)
                {
                    Logger.LogDebug("Rejected vote from " + requestVoteRPC.CandidateId + " as current term is greater (" + requestVoteRPC.Term + "<" + _nodeStorage.CurrentTerm + ")");
                }
                else if (requestVoteRPC.LastLogIndex < _nodeStorage.GetLogCount() - 1)
                {
                    Logger.LogDebug("Rejected vote from " + requestVoteRPC.CandidateId + " as last log index is less then local index (" + requestVoteRPC.LastLogIndex + "<" + (_nodeStorage.GetLogCount() - 1) + ")");
                }
                else if (requestVoteRPC.LastLogTerm < _nodeStorage.GetLastLogTerm())
                {
                    Logger.LogDebug("Rejected vote from " + requestVoteRPC.CandidateId + " as last log term is less then local term (" + requestVoteRPC.LastLogTerm + "<" + _nodeStorage.GetLastLogTerm() + ")");
                }
            }
            return new RequestVoteResponse()
            {
                Success = successful
            };
        }

        public AppendEntryResponse AppendEntryRPCHandler(AppendEntry entry)
        {
            //Check the log check to prevent a intermittent term increase with no back tracking, TODO check whether this causes potentially concurrency issues
            if (entry.Term < _nodeStorage.CurrentTerm && entry.LeaderCommit < CommitIndex)
            {
                Logger.LogInformation("Rejected RPC from " + entry.LeaderId + " due to lower term");
                return new AppendEntryResponse()
                {
                    ConflictName = "Old Term Name",
                    Successful = false
                };
            }

            //Reset the timer if the append is from a valid term
            ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);

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

            if (!InCluster)
            {
                InCluster = true;
            }

            DateTime time = DateTime.Now;

            foreach (var log in entry.Entries)
            {
                _nodeStorage.AddLog(log);
            }

            if (entry.LeaderCommit > LatestLeaderCommit)
            {
                Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                LatestLeaderCommit = entry.LeaderCommit;
            }

            return new AppendEntryResponse()
            {
                Successful = true
            };
        }

        public async Task<RequestDataShardResponse> RequestDataShardHandler(RequestDataShard request)
        {
            bool found = false;
            RequestDataShardResponse data = null;
            var currentTime = DateTime.Now;
            Guid? FoundShard = null;
            Guid? FoundOnNode = null;

            if (!_stateMachine.IndexExists(request.Type))
            {
                return new RequestDataShardResponse()
                {
                    IsSuccessful = true,
                    SearchMessage = "The type " + request.Type + " does not exist."
                };
            }

            if (request.ShardId == null)
            {
                if (request.CreateLock)
                {
                    try
                    {
                        if (!_stateMachine.IsObjectLocked(request.ObjectId))
                        {
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>{
                                new SetObjectLock()
                                {
                                    ObjectId = request.ObjectId,
                                    Type = request.Type
                                }
                        },
                                WaitForCommits = true
                            });
                        }
                        else
                        {
                            throw new ConflictingObjectLockException();
                        }
                    }
                    catch (ConflictingObjectLockException e)
                    {
                        return new RequestDataShardResponse()
                        {
                            IsSuccessful = true,
                            IsLocked = true,
                            SearchMessage = "Object " + request.ObjectId + " is locked."
                        };
                    }

                }

                var shards = _stateMachine.GetAllPrimaryShards(request.Type);

                bool foundResult = false;
                ShardData finalObject = null;
                var totalRespondedShards = 0;

                var tasks = shards.Select(async shard =>
                {
                    if (shard.Value != _nodeStorage.Id)
                    {
                        try
                        {
                            var result = await NodeConnectors[_stateMachine.CurrentState.Nodes[shard.Value].TransportAddress].Send(new RequestDataShard()
                            {
                                ObjectId = request.ObjectId,
                                ShardId = shard.Key, //Set the shard
                                Type = request.Type
                            });

                            if (result.IsSuccessful)
                            {
                                foundResult = true;
                                finalObject = result.Data;
                                FoundShard = result.ShardId;
                                FoundOnNode = result.NodeId;
                            }

                            Interlocked.Increment(ref totalRespondedShards);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError("Error thrown while getting " + e.Message);
                        }
                    }
                    else
                    {
                        finalObject = _dataRouter.GetData(request.Type, request.ObjectId);
                        foundResult = finalObject != null ? true : false;
                        FoundShard = shard.Key;
                        FoundShard = shard.Value;
                        Interlocked.Increment(ref totalRespondedShards);
                    }
                });

                Task.WhenAll(tasks);
                //Parallel.ForEach(tasks, task => task.Start());

                while (!foundResult && totalRespondedShards < shards.Count)
                {
                    if ((DateTime.Now - currentTime).TotalMilliseconds > request.TimeoutMs)
                    {
                        return new RequestDataShardResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                    Thread.Sleep(10);
                }

                return new RequestDataShardResponse()
                {
                    IsSuccessful = true,
                    Data = finalObject,
                    SearchMessage = finalObject != null ? null : "Object " + request.ObjectId + " could not be found in shards."
                };
            }
            else
            {
                var finalObject = _dataRouter.GetData(request.Type, request.ObjectId);
                return new RequestDataShardResponse()
                {
                    IsSuccessful = finalObject != null,
                    Data = finalObject,
                    NodeId = FoundOnNode,
                    ShardId = FoundShard,
                    SearchMessage = finalObject != null ? null : "Object " + request.ObjectId + " could not be found in shards.",
                };
            }
        }
        #endregion

        #region Internal Parallel Calls
        public async void SendHeartbeats()
        {
            Logger.LogDebug("Sending heartbeats");
            var startTime = DateTime.Now;
            var tasks = NodeConnectors.Select(async connector =>
            {
                try
                {
                    Logger.LogDebug("Sending heartbeat to " + connector.Key);
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
                    Logger.LogWarning("Encountered error while sending heartbeat to node " + connector.Key + ", request failed with error \"" + e.Message);//+ "\"" + e.StackTrace);
                }
            });

            await Task.WhenAll(tasks);

            /*bool pendingTasks = true;

            while (pendingTasks)
            {
                if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.LatencyToleranceMs)
                {
                    Logger.LogError("Sending heartbeats timed out");
                    break;
                }
                pendingTasks = false;
                foreach (var task in tasks)
                {
                    if (!task.IsCompleted)
                    {
                        pendingTasks = true;
                    }
                }
            }*/
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
                Console.WriteLine("Node's role changed to " + newState.ToString());
                CurrentState = newState;

                switch (newState)
                {
                    case NodeState.Candidate:
                        _nodeStorage.UpdateCurrentTerm(_nodeStorage.CurrentTerm + 1);
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        InCluster = false;
                        break;
                    case NodeState.Follower:
                        //On becoming a follower, wait 5 seconds to allow any other nodes to send out election time outs
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs * 4, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        RestartThread(ref _taskWatchThread, () => GetTaskWatchThread());
                        RestartThread(ref _commitThread, () => GetCommitThread());
                        RestartThread(ref _nodeSelfHealingThread, () => NodeSelfHealingThread());
                        break;
                    case NodeState.Leader:
                        CompletedFirstLeaderDiscovery = false;
                        CurrentLeader = new KeyValuePair<Guid?, string>(_nodeStorage.Id, MyUrl);
                        ResetLeaderState();
                        ResetTimer(_heartbeatTimer, 0, _clusterOptions.ElectionTimeoutMs / 2);
                        ResetTimer(_clusterInfoTimeoutTimer, 0, 1000);
                        StopTimer(_electionTimeoutTimer);
                        RestartThread(ref _commitThread, () => GetCommitThread());
                        RestartThread(ref _indexCreationThread, () => GetIndexCreationThread());
                        RestartThread(ref _taskWatchThread, () => GetTaskWatchThread());
                        InCluster = true;
                        RestartThread(ref _nodeSelfHealingThread, () => NodeSelfHealingThread());
                        RestartThread(ref _shardReassignmentThread, () => GetShardReassignmentThread());
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
                    var eligbleNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Value.IsContactable).ToDictionary(k => k.Key, v => v.Value);
                    var rand = new Random();
                    while (eligbleNodes.Count() == 0)
                    {
                        Logger.LogWarning("No eligible nodes found, awaiting eligible nodes.");
                        Thread.Sleep(1000);
                        eligbleNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Value.IsContactable).ToDictionary(k => k.Key, v => v.Value);
                    }

                    //var selectedNode = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count()));
                    //Logger.LogDebug("Allocating data shart to node at " + selectedNode.Key);

                    List<SharedShardMetadata> Shards = new List<SharedShardMetadata>();

                    for (var i = 0; i < _clusterOptions.NumberOfShards; i++)
                    {
                        Shards.Add(new SharedShardMetadata()
                        {
                            InsyncAllocations = eligbleNodes.Keys.ToList(),//new List<Guid> { selectedNode.Key },
                            PrimaryAllocation = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count())).Key,
                            Id = Guid.NewGuid(),
                            Type = type
                        });

                        foreach (var allocationI in Shards[i].InsyncAllocations)
                        {
                            if (allocationI != _nodeStorage.Id)
                            {
                                await NodeConnectors[_stateMachine.CurrentState.Nodes[allocationI].TransportAddress].Send(new RequestInitializeNewShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                });
                            }
                            else
                            {
                                await Send(new RequestInitializeNewShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                });
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

        /// <summary>
        /// Sync the local shard with the clusters shard. Used mainly if the node is out of sync with the cluster.
        /// </summary>
        /// <param name="shardId"></param>
        /// <returns></returns>
        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            LocalShardMetaData shard;
            //Check if shard exists, if not create it.
            if ((shard = _nodeStorage.GetShardMetadata(shardId)) == null)
            {
                //Create local empty shard
                _nodeStorage.AddNewShardMetaData(new LocalShardMetaData()
                {
                    ShardId = shardId,
                    ShardOperations = new ConcurrentDictionary<int, ShardOperation>(),
                    Type = type
                });
                shard = _nodeStorage.GetShardMetadata(shardId);
            }

            //you should sync twice, first to sync from stale, then to sync any trailing data
            var syncs = 0;
            Random rand = new Random();

            while (syncs < 2)
            {
                var shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
                var latestPrimary = await NodeConnectors[_stateMachine.CurrentState.Nodes[shardMetadata.InsyncAllocations[rand.Next(0, shardMetadata.InsyncAllocations.Where(i => i != _nodeStorage.Id).Count())]].TransportAddress].Send(new RequestShardOperations()
                {
                    From = shard.SyncPos + 1,
                    To = shard.SyncPos + 1,
                    ShardId = shardId,
                    Type = type
                });

                var currentPos = _nodeStorage.GetShardMetadata(shardId).SyncPos;
                var lastPrimaryPosition = latestPrimary.LatestPosition;
                while (currentPos < lastPrimaryPosition)
                {
                    var nextOperations = await NodeConnectors[_stateMachine.CurrentState.Nodes[shardMetadata.InsyncAllocations[rand.Next(0, shardMetadata.InsyncAllocations.Count())]].TransportAddress].Send(new RequestShardOperations()
                    {
                        From = shard.SyncPos + 1,
                        // do 100 records at a time
                        To = shard.SyncPos + 100,
                        ShardId = shardId,
                        Type = type
                    });

                    foreach (var operation in nextOperations.Operations)
                    {
                        await Send(new ReplicateShardOperation()
                        {
                            Operation = new ShardOperation()
                            {
                                Operation = operation.Value.Operation,
                                ObjectId = operation.Value.ObjectId
                            },
                            Payload = (ShardData)operation.Value.Payload,
                            Pos = operation.Key,
                            ShardId = shard.ShardId
                        });
                    }

                    Logger.LogDebug("Synced shard " + shard.ShardId + " upto " + nextOperations.Operations.Last().Key);

                    currentPos = _nodeStorage.GetShardMetadata(shardId).SyncPos;
                    lastPrimaryPosition = nextOperations.LatestPosition;
                }

                //Only need to reassign insync node on first operation
                if (syncs == 0)
                {
                    Logger.LogDebug("Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
                    var newListOfInsync = shardMetadata.InsyncAllocations;
                    if (!newListOfInsync.Contains(_nodeStorage.Id))
                    {
                        newListOfInsync.Add(_nodeStorage.Id);

                        //Update cluster to make this node in-sync
                        await Send(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>()
                        {
                            new UpdateShardMetadata(){
                                InsyncAllocations = newListOfInsync,
                                PrimaryAllocation = shardMetadata.PrimaryAllocation,
                                ShardId = shardMetadata.Id,
                                //Remove this node from stale operations
                                StaleAllocations = shardMetadata.StaleAllocations.Where(id => id != _nodeStorage.Id).ToList(),
                                Type = shardMetadata.Type
                            }
                        },
                            WaitForCommits = true
                        });

                        //Sleep for 5 seconds to allow for any uncommunicated transactions to catch up
                        Thread.Sleep(5000);
                    }
                    while (!_stateMachine.GetShardMetadata(shard.ShardId, shard.Type).InsyncAllocations.Contains(_nodeStorage.Id))
                    {
                        Thread.Sleep(1000);
                    }
                }
                syncs++;
            }
            return true;
        }
    }
}