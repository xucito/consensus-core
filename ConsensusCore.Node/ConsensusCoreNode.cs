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
        private Thread _bootstrapThread;
        private Thread _nodeSelfHealingThread;
        private Thread _shardReassignmentThread;

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
        /// Ids of all threads syncing a shard
        /// </summary>
        public ConcurrentDictionary<Guid, Task> SyncThreads = new ConcurrentDictionary<Guid, Task>();

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
                    Id = _nodeStorage.Id,
                    InCluster = InCluster,
                    Status = GetNodeStatus(),
                    CommitIndex = CommitIndex,
                    ShardOperationCounts = _nodeStorage.GetShardOperationCounts(),
                    ShardSyncPositions = _nodeStorage.GetShardSyncPositions()
                };
            }
        }

        public NodeStatus GetNodeStatus()
        {
            if (!InCluster)
            {
                return NodeStatus.Red;
            }

            if (CommitIndex < LatestLeaderCommit || _stateMachine.GetAllOutOfSyncShards(_nodeStorage.Id).Count() > 0 || AreThereRecoveryJobs())
            {
                return NodeStatus.Yellow;
            }

            //Node is in cluster and all shards are synced
            return NodeStatus.Green;
        }

        public bool AreThereRecoveryJobs()
        {
            foreach (var job in SyncThreads)
            {
                if (!job.Value.IsCompleted)
                {
                    return true;
                }
            }
            return false;
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
                MyUrl = "https://localhost:5022";
            }
            _dataRouter = dataRouter;
            if (dataRouter != null)
            {
                Logger.LogDebug(GetNodeId() + "Data routing has been enabled on this node");
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
                        Logger.LogWarning(GetNodeId() + "Leader was not found in cluster, routing via this node may fail... will sleep and try again..");
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        Logger.LogDebug(GetNodeId() + "Leader was found at URL " + node.TransportAddress);
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
                    //Only apply self healing if your state is upto date
                    if (InCluster && LatestLeaderCommit <= CommitIndex)
                    {
                        Logger.LogDebug(GetNodeId() + "Starting self healing thread.");
                        //This will get you all the out of sync shards
                        var outOfSyncShards = _stateMachine.GetAllOutOfSyncShards(_nodeStorage.Id);

                        if (outOfSyncShards.Count() > 0)
                        {
                            //foreach out of sync, resync the threads
                            foreach (var shard in outOfSyncShards)
                            {
                                try
                                {
                                    Interlocked.Increment(ref syncThreads);
                                    Logger.LogInformation(GetNodeId() + "Start sync. Current threads being synced " + syncThreads);
                                    //Console.WriteLine("Start sync for shard " + shard.Id + ". Current threads being synced " + syncThreads);
                                    AddShardSyncTask(shard.Id, shard.Type);
                                    Logger.LogInformation(GetNodeId() + "Resynced " + shard.Id + " successfully.");
                                    //Console.WriteLine("Resynced " + shard.Id + " successfully.");
                                }
                                catch (Exception e)
                                {
                                    Logger.LogError("Failed to sync shard " + shard.Id + " with error " + e.StackTrace);
                                }
                                Interlocked.Decrement(ref syncThreads);
                            };

                            Logger.LogInformation(GetNodeId() + "Resynced all shards successfully.");
                        }
                    }
                    else
                    {
                        Logger.LogDebug(GetNodeId() + "Skipping self healing thread as node is not reporting in cluster.");

                        //  Console.WriteLine("Skipping self healing thread as node is not reporting in cluster.");
                    }
                    Thread.Sleep(1000);
                }
            });
        }

        public async void AddShardSyncTask(Guid shardId, string type)
        {
            try
            {
                if (!SyncThreads.ContainsKey(shardId))
                {
                    if (SyncThreads.TryAdd(shardId, SyncShard(shardId, type)))
                    {
                        Logger.LogDebug(GetNodeId() + "Adding sync thread for shard " + shardId);
                        //  Console.WriteLine("Added sync job for " + shardId);
                    }
                    else
                    {
                        Logger.LogError("Failed to add new sync thread");
                    }
                }
                else if (SyncThreads[shardId].IsCompleted)
                {

                    if (SyncThreads.TryRemove(shardId, out _) && SyncThreads.TryAdd(shardId, SyncShard(shardId, type)))
                    {
                        Logger.LogDebug(GetNodeId() + "Restarted sync " + SyncThreads);
                    }
                }
                else
                {
                    Logger.LogDebug(GetNodeId() + "Sync for " + shardId + " already happening.");
                }
            }
            catch (Exception e)
            {
                Logger.LogError(GetNodeId() + "Failed to add shard sync task for shard " + shardId + "with error " + e.StackTrace);
            }
        }

        public async Task<bool> BootstrapNode()
        {
            Logger.LogInformation(GetNodeId() + "Bootstrapping Node!");

            // The node cannot bootstrap unless at least a majority of nodes are present

            //while (NodeConnectors.Count() < _clusterOptions.MinimumNodes - 1)
            //{
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
                    Logger.LogWarning(GetNodeId() + "Node at url " + url + " was unreachable...");
                }
            }

            if (MyUrl == null)
            {
                Logger.LogWarning(GetNodeId() + "Node is not discoverable from the given node urls!");
            }

            if (NodeConnectors.Count() < _clusterOptions.MinimumNodes - 1)
            {
                Logger.LogWarning(GetNodeId() + "Not enough of the nodes in the cluster are contactable, awaiting bootstrap");
            }
            //}
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

                            Logger.LogInformation(GetNodeId() + "Added " + newStaleAllocations.Count() + " nodes to " + shard.Id);
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
                                Logger.LogInformation(GetNodeId() + "Creating index for type " + typeToCreate);
                                if (CurrentState == NodeState.Leader)
                                    CreateIndex(typeToCreate);

                                while (!_stateMachine.IndexExists(typeToCreate))
                                {
                                    Logger.LogInformation(GetNodeId() + "Awaiting index creation.");
                                    Thread.Sleep(100);
                                }
                            }
                            else
                            {
                                Logger.LogDebug(GetNodeId() + "INDEX for type " + typeToCreate + " Already exists");
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
                Logger.LogDebug(GetNodeId() + "Rediscovering nodes...");
                List<Guid> NodesToMarkAsStale = new List<Guid>();
                List<Guid> NodesToRemove = new List<Guid>();
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
                            Logger.LogDebug(GetNodeId() + "Detected updated for node " + nodeId);
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
                                var conflictingNodeId = conflictingNodes.First().Key;
                                Logger.LogWarning(GetNodeId() + "Detected another node with conflicting transport address, removing the conflicting node from the cluster");
                                nodeUpsertCommands.Add(new DeleteNodeInformation()
                                {
                                    Id = conflictingNodes.First().Key
                                });
                                NodesToRemove.Add(conflictingNodeId);

                                /*foreach (var shard in _stateMachine.GetShards(conflictingNodeId))
                                {
                                    nodeUpsertCommands.Add(new UpdateShardMetadata()
                                    {
                                        ShardId = shard.Id,
                                        InsyncAllocations = shard.InsyncAllocations.Where(id => id != conflictingNodeId).ToList(),
                                        StaleAllocations = shard.StaleAllocations.Where(id => id != conflictingNodeId).ToList(),
                                        PrimaryAllocation = shard.PrimaryAllocation,
                                        Type = shard.Type
                                    });
                                }*/
                            }

                            /* if(!NodeConnectors.ContainsKey(url))
                             {
                                 NodeConnectors.Add(url, new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs)));
                             }*/
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning(GetNodeId() + "Node at url " + url + " was unreachable...");
                        var staleNode = _stateMachine.GetNode(url);
                        if (staleNode != null)
                            NodesToMarkAsStale.Add(staleNode.Id);
                    }
                }
                var rand = new Random();

                if (NodesToMarkAsStale.Count() > 0 || NodesToRemove.Count() > 0)
                {
                    Logger.LogWarning(GetNodeId() + "Found stale or removed nodes, reassigning all nodes");

                    foreach (var shard in _stateMachine.GetShards())
                    {
                        if (NodesToMarkAsStale.Contains(shard.PrimaryAllocation) || shard.InsyncAllocations.Where(i => NodesToRemove.Contains(i)).Count() > 0 || shard.StaleAllocations.Where(i => NodesToRemove.Contains(i)).Count() > 0)
                        {
                            Logger.LogDebug(GetNodeId() + "Reassigned shard " + shard.Id);
                            var realInsyncShards = shard.InsyncAllocations.Where(id => !NodesToRemove.Contains(id) && !NodesToMarkAsStale.Contains(id)).ToList();
                            var realStaleShards = shard.StaleAllocations.Where(id => !NodesToRemove.Contains(id)).ToList();
                            realStaleShards.AddRange(NodesToMarkAsStale);

                            nodeUpsertCommands.Add(new UpdateShardMetadata()
                            {
                                PrimaryAllocation = !NodesToMarkAsStale.Contains(shard.PrimaryAllocation) && !NodesToRemove.Contains(shard.PrimaryAllocation) ? shard.PrimaryAllocation : realInsyncShards.ElementAt(rand.Next(0, realInsyncShards.Count)),
                                InsyncAllocations = realInsyncShards,
                                StaleAllocations = realStaleShards,
                                ShardId = shard.Id,
                                Type = shard.Type
                            });
                        }
                    }
                }


                foreach (var nodeId in NodesToMarkAsStale)
                {
                    if (_stateMachine.IsNodeContactable(nodeId))
                    {
                        nodeUpsertCommands.Add(new UpsertNodeInformation()
                        {
                            IsContactable = false,
                            Id = nodeId,
                            TransportAddress = _stateMachine.GetNodes().Where(n => n.Id == nodeId).First().TransportAddress
                        });
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
            var acquiredLock = false;
            try
            {
                Random rand = new Random();
                acquiredLock = Monitor.TryEnter(VoteLock);
                if (IsBootstrapped && acquiredLock)
                {
                    Thread.Sleep(rand.Next(0, 350));
                    SetNodeRole(NodeState.Candidate);
                }
            }
            finally
            {
                if (acquiredLock)
                {
                    Monitor.Exit(VoteLock);
                }
            }
        }

        public void HeartbeatTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                Logger.LogDebug(GetNodeId() + "Detected heartbeat timeout event.");
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
                Logger.LogWarning(GetNodeId() + "Could not find node " + id.ToString());
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

                Logger.LogDebug(GetNodeId() + "Detected RPC " + request.GetType().Name + ".");
                if (!IsBootstrapped)
                {
                    Logger.LogDebug(GetNodeId() + "Node is not ready...");
                    return default(TResponse);
                }

                if (IsClusterRequest<TResponse>(request) && !InCluster)
                {
                    Logger.LogWarning(GetNodeId() + "Reqeuest rejected, node is not apart of cluster...");
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
                Logger.LogError("Failed request " + request.RequestName + " with error " + e.StackTrace);
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
                case WriteData t1:
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
                LatestPosition = _nodeStorage.GetCurrentShardLatestCount(request.ShardId),
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
                        Logger.LogWarning(GetNodeId() + "Currently a candidate during routing, will sleep thread and try again.");
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
                        Logger.LogDebug(GetNodeId() + "Detected routing of command " + request.GetType().Name + " to leader.");
                        return (TResponse)(object)await GetLeadersConnector().Send(request);
                    }
                    catch (Exception e)
                    {
                        Logger.LogDebug(GetNodeId() + "Encountered " + e.Message + " while trying to route " + request.GetType().Name + " to leader.");
                    }
                }
            }
            return Handle();
        }

        public ReplicateShardOperationResponse ReplicateShardOperationHandler(ReplicateShardOperation request)
        {
            try
            {
                Logger.LogInformation(GetNodeId() + "Recieved replication request for shard" + request.ShardId + " for object " + request.Operation.ObjectId + " for action " + request.Operation.Operation.ToString() + " operation " + request.Pos);
                /*if (request.Operation.Operation == ShardOperationOptions.Update || request.Operation.Operation == ShardOperationOptions.Delete)
                {
                    var startTime = DateTime.Now;
                    while ((request.Pos - 1) > LocalShards[request.ShardId].SyncPos)
                    {
                        Logger.LogDebug(GetNodeId() + "Waiting for log to sync up with position " + (request.Pos - 1) + " current posituion " + LocalShards[request.ShardId].SyncPos);
                        Thread.Sleep(100);

                        AddShardSyncTask(request.ShardId, request.Type);

                        if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                        {
                            Logger.LogError("Update function timed out " + (request.Pos - 1));
                            return new ReplicateShardOperationResponse()
                            {
                                IsSuccessful = false
                            };
                        }
                    }
                }*/

                if (_nodeStorage.ReplicateShardOperation(request.ShardId, request.Pos, request.Operation))
                {
                    if (!RunDataOperation(request.Operation.Operation, request.Payload))
                    {
                        Logger.LogError("Ran into error while running operation " + request.Operation.ToString() + " on " + request.ShardId);
                        if (!_nodeStorage.RemoveOperation(request.ShardId, request.Pos))
                        {
                            Logger.LogError("Ran into critical error when rolling back operation " + request.Pos + " on shard " + request.ShardId);
                        }
                        return new ReplicateShardOperationResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                    else
                    {
                       /* if (_nodeStorage.GetShardMetadata(request.ShardId).SyncPos < request.Pos - 1)
                        {
                            Logger.LogInformation(GetNodeId() + "Detected delayed sync position, sending recovery command.");
                            AddShardSyncTask(request.ShardId, request.Type);
                        }*/
                        _nodeStorage.MarkOperationAsCommited(request.ShardId, request.Pos);
                        Logger.LogInformation(GetNodeId() + "Marked operation " + request.Pos + " on shard " + request.ShardId + "as commited");
                    }
                }
                Logger.LogInformation(GetNodeId() + "Successfully replicated request for shard" + request.ShardId + " for object " + request.Operation.ObjectId + " for action " + request.Operation.Operation.ToString());
                return new ReplicateShardOperationResponse()
                {
                    LatestPosition = LocalShards[request.ShardId].LatestShardOperation,
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
            //Add
            int index = _nodeStorage.AddCommands(request.Commands.ToList(), _nodeStorage.CurrentTerm);
            var startDate = DateTime.Now;
            while (request.WaitForCommits)
            {
                if ((DateTime.Now - startDate).TotalMilliseconds > _clusterOptions.CommitsTimeout)
                {
                    return new ExecuteCommandsResponse()
                    {
                        IsSuccessful = false
                    };
                }

                Logger.LogDebug(GetNodeId() + "Waiting for " + request.RequestName + " to complete.");
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

        public bool RunDataOperation(ShardOperationOptions operation, ShardData shard)
        {
            try
            {
                switch (operation)
                {
                    case ShardOperationOptions.Create:
                        _dataRouter.InsertData(shard);
                        break;
                    case ShardOperationOptions.Update:
                        if (!_nodeStorage.IsObjectMarkedForDeletion(shard.ShardId.Value, shard.Id))
                        {
                            _dataRouter.UpdateData(shard);
                        }
                        else
                        {
                            Console.WriteLine("OBJECT IS MARKED FOR DELETION");
                        }
                        break;
                    case ShardOperationOptions.Delete:
                        if (_nodeStorage.MarkShardForDeletion(shard.ShardId.Value, shard.Id))
                        {
                            _dataRouter.DeleteData(shard);
                        }
                        else
                        {
                            Logger.LogError("Ran into error while deleting " + shard.Id);
                            return false;
                        }
                        break;
                }
                return true;
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to run data operation against shard " + shard.Id);
                return false;
            }
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
            Logger.LogInformation(GetNodeId() + "Received write request for object " + shard.Data.Id + " for shard " + shard.Data.ShardId);
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
                if (_nodeStorage.GetShardMetadata(shard.Data.ShardId.Value) == null)
                {
                    Logger.LogInformation(GetNodeId() + "Creating local copy of shard");
                    _nodeStorage.AddNewShardMetaData(new LocalShardMetaData()
                    {
                        ShardId = shard.Data.ShardId.Value,
                        ShardOperations = new ConcurrentDictionary<int, ShardOperation>(),
                        Type = shard.Data.Type
                    });
                }

                //Commit the sequence Number
                int sequenceNumber = _nodeStorage.AddNewShardOperation(shard.Data.ShardId.Value, new ShardOperation()
                {
                    ObjectId = shard.Data.Id,
                    Operation = shard.Operation
                });

                //Write to the replicated nodes
                List<Guid> InvalidNodes = new List<Guid>();

                bool successfullyMarkedOutOfSync = false;
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
                            Pos = sequenceNumber,
                            Type = shard.Data.Type
                        });

                        if (result.IsSuccessful)
                        {
                            Logger.LogDebug(GetNodeId() + "Successfully replicated all " + shardMetadata.Id + "shards.");
                        }
                        else
                        {
                            throw new Exception("Failed to replicate data to shard " + shardMetadata.Id + " to node " + allocation);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(GetNodeId() + "Failed to replicate shard " + shardMetadata.Id + " for operation " + sequenceNumber + ", marking shard as not insync..." + e.StackTrace);
                        InvalidNodes.Add(allocation);
                    }
                }


                if (InvalidNodes.Count() > 0)
                {
                    HashSet<Guid> allStaleAllocations = shardMetadata.StaleAllocations.ToHashSet();
                    foreach (var node in InvalidNodes)
                        allStaleAllocations.Add(node);
                    Logger.LogInformation(GetNodeId() + "Detected invalid nodes, setting nodes to be out-of-sync");

                    var result = await Send(new ExecuteCommands()
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
                        },
                        WaitForCommits = true
                    });

                    successfullyMarkedOutOfSync = result.IsSuccessful;
                }
                else
                {
                    successfullyMarkedOutOfSync = true;
                }

                if (successfullyMarkedOutOfSync && !RunDataOperation(shard.Operation, shard.Data))
                {
                    Logger.LogError("Ran into error while running operation " + shard.Operation.ToString() + " on " + shard.Data.Id);
                    if (!_nodeStorage.RemoveOperation(shard.Data.ShardId.Value, sequenceNumber))
                    {
                        Logger.LogError("Ran into critical error when rolling back operation " + sequenceNumber + " on shard " + shard.Data.ShardId.Value);
                    }
                    return new WriteDataResponse()
                    {
                        IsSuccessful = false
                    };
                }
                else
                {
                    if (!successfullyMarkedOutOfSync)
                    {
                        Logger.LogCritical(GetNodeId() + "Failed to mark invalid nodes as out-of-sync, I may be out of the cluster and will revert this write!");
                    }
                    //If the shard metadata is not synced upto date
                    if (_nodeStorage.GetShardMetadata(shard.Data.ShardId.Value).SyncPos < sequenceNumber - 1)
                    {
                        Logger.LogInformation(GetNodeId() + "Detected delayed sync position, sending recovery command.");
                        AddShardSyncTask(shard.Data.Id, shard.Data.Type);
                    }
                    _nodeStorage.MarkOperationAsCommited(shard.Data.ShardId.Value, sequenceNumber);
                }
                /*switch (shard.Operation)
                {
                    case ShardOperationOptions.Create:
                        _dataRouter.InsertData(shard.Data);
                        break;
                    case ShardOperationOptions.Update:
                        _dataRouter.UpdateData(shard.Data);
                        break;
                    case ShardOperationOptions.Delete:
                        if (_nodeStorage.MarkShardForDeletion(shardMetadata.Id, shard.Data.Id))
                        {
                            _dataRouter.DeleteData(shard.Data);
                        }
                        else
                        {
                            Logger.LogError("Ran into error while deleting " + shard.Data.Id);
                            return new WriteDataResponse()
                            {
                                IsSuccessful = false
                            };
                        }
                        break;
                }*/



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
                    Logger.LogError(GetNodeId() + "Failed to write " + shard.Operation.ToString() + " request to primary node " + _stateMachine.CurrentState.Nodes[shardMetadata.PrimaryAllocation].TransportAddress + " for object " + shard.Data.Id + " shard " + shard.Data.ShardId + "|" + e.StackTrace);
                    throw e;
                }
                return new WriteDataResponse()
                {
                    IsSuccessful = true,
                    ShardId = shard.Data.ShardId.Value
                };
            }
        }

        object VoteLock = new object();

        public RequestVoteResponse RequestVoteRPCHandler(RequestVote requestVoteRPC)
        {
            var successful = false;
            if (IsBootstrapped)
            {
                //To requests might come in at the same time causing the VotedFor to not match
                lock (VoteLock)
                {
                    //Ref1 $5.2, $5.4
                    if (_nodeStorage.CurrentTerm <= requestVoteRPC.Term && ((_nodeStorage.VotedFor == null || _nodeStorage.VotedFor == requestVoteRPC.CandidateId) &&
                    (requestVoteRPC.LastLogIndex >= _nodeStorage.GetLogCount() && requestVoteRPC.LastLogTerm >= _nodeStorage.GetLastLogTerm())))
                    {
                        _nodeStorage.SetVotedFor(requestVoteRPC.CandidateId);
                        Logger.LogInformation(GetNodeId() + "Voting for " + requestVoteRPC.CandidateId + " for term " + requestVoteRPC.Term);
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        SetCurrentTerm(requestVoteRPC.Term);
                        successful = true;
                    }
                    else if (_nodeStorage.CurrentTerm > requestVoteRPC.Term)
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as current term is greater (" + requestVoteRPC.Term + "<" + _nodeStorage.CurrentTerm + ")");
                    }
                    else if (requestVoteRPC.LastLogIndex < _nodeStorage.GetLogCount() - 1)
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as last log index is less then local index (" + requestVoteRPC.LastLogIndex + "<" + (_nodeStorage.GetLogCount() - 1) + ")");
                        //If you determine that your log index is greater but your term is lower, increment your term to one level higher
                        /*   if (requestVoteRPC.Term > _nodeStorage.CurrentTerm)
                           {
                               Logger.LogWarning(GetNodeId() + "Fast forwarding term.");
                               SetCurrentTerm(requestVoteRPC.Term);
                           }**/
                    }
                    else if (requestVoteRPC.LastLogTerm < _nodeStorage.GetLastLogTerm())
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as last log term is less then local term (" + requestVoteRPC.LastLogTerm + "<" + _nodeStorage.GetLastLogTerm() + ")");
                    }
                    else if ((_nodeStorage.VotedFor != null && _nodeStorage.VotedFor != requestVoteRPC.CandidateId))
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as I have already voted for " + _nodeStorage.VotedFor);
                    }
                    else if (!successful)
                    {
                        Logger.LogError("Rejected vote from " + requestVoteRPC.CandidateId + " due to unknown reason.");
                    }
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
            if (entry.Term < _nodeStorage.CurrentTerm && entry.LeaderCommit <= CommitIndex)
            {
                Logger.LogInformation(GetNodeId() + "Rejected RPC from " + entry.LeaderId + " due to lower term " + entry.Term + "<" + _nodeStorage.CurrentTerm);
                return new AppendEntryResponse()
                {
                    ConflictName = "Old Term Name",
                    Successful = false
                };
            }

            //Reset the timer if the append is from a valid term
            ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);

            var previousEntry = _nodeStorage.GetLogAtIndex(entry.PrevLogIndex);

            if (previousEntry == null && entry.PrevLogIndex != 0)
            {
                Logger.LogWarning(GetNodeId() + "Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");

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
                Logger.LogWarning(GetNodeId() + "Inconsistency found in the node logs and leaders logs, log " + entry.PrevLogTerm + " from term " + entry.PrevLogTerm + " does not exist.");
                var logs = _nodeStorage.Logs.Where(l => l.Term == entry.PrevLogTerm).FirstOrDefault();
                return new AppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.ConflictingLogEntryException,
                    Successful = false,
                    ConflictingTerm = entry.PrevLogTerm,
                    FirstTermIndex = logs != null ? logs.Index : 0
                };
            }

            SetCurrentTerm(entry.Term);

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
                Logger.LogDebug(GetNodeId() + "Detected uncontacted leader, discovering leader now.");
                //Reset the current leader
                CurrentLeader = new KeyValuePair<Guid?, string>(entry.LeaderId, null);
                _findLeaderThread = FindLeaderThread(entry.LeaderId);
                _findLeaderThread.Start();
            }

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
                Logger.LogDebug(GetNodeId() + "Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
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
            Logger.LogDebug(GetNodeId() + "Sending heartbeats");
            var startTime = DateTime.Now;
            var tasks = NodeConnectors.Select(async connector =>
            {
                try
                {
                    Logger.LogDebug(GetNodeId() + "Sending heartbeat to " + connector.Key);
                    var entriesToSend = new List<LogEntry>();

                    var prevLogIndex = Math.Max(0, NextIndex[connector.Key] - 1);
                    int prevLogTerm = (_nodeStorage.GetLogCount() > 0 && prevLogIndex > 0) ? prevLogTerm = _nodeStorage.GetLogAtIndex(prevLogIndex).Term : 0;

                    if (NextIndex[connector.Key] <= _nodeStorage.GetLastLogIndex() && _nodeStorage.GetLastLogIndex() != 0 && !LogsSent.GetOrAdd(connector.Key, true))
                    {
                        var unsentLogs = (_nodeStorage.GetLogCount() - NextIndex[connector.Key] + 1);
                        var quantityToSend = unsentLogs;
                        entriesToSend = _nodeStorage.Logs.GetRange(NextIndex[connector.Key] - 1, quantityToSend < maxSendEntries ? quantityToSend : maxSendEntries).ToList();
                        // entriesToSend = _nodeStorage.Logs.Where(l => l.Index >= NextIndex[connector.Key]).ToList();
                        Logger.LogDebug(GetNodeId() + "Detected node " + connector.Key + " is not upto date, sending logs from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index);
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
                        Logger.LogWarning(GetNodeId() + "Detected node " + connector.Value + " is missing the previous log, sending logs from log " + result.LastLogEntryIndex.Value + 1);
                        NextIndex[connector.Key] = result.LastLogEntryIndex.Value + 1;
                    }
                    else if (result.ConflictName == AppendEntriesExceptionNames.ConflictingLogEntryException)
                    {
                        var firstEntryOfTerm = _nodeStorage.Logs.Where(l => l.Term == result.ConflictingTerm).FirstOrDefault();
                        var revertedIndex = firstEntryOfTerm.Index < result.FirstTermIndex ? firstEntryOfTerm.Index : result.FirstTermIndex.Value;
                        Logger.LogWarning(GetNodeId() + "Detected node " + connector.Value + " has conflicting values, reverting to " + revertedIndex);

                        //Revert back to the first index of that term
                        NextIndex[connector.Key] = revertedIndex;
                    }
                    else
                    {
                        var node = _stateMachine.CurrentState.Nodes.Where(n => n.Value.TransportAddress == connector.Key);
                        if (node.Count() == 1)
                        {
                            //Mark the node as uncontactable
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>()
                            {
                                new UpsertNodeInformation()
                                {
                                    IsContactable = false,
                                    Id = node.First().Value.Id,
                                    TransportAddress = connector.Key
                                }
                            }
                            });
                        }
                        throw new Exception("Append entry returned with undefined conflict name");
                    }
                }
                catch (Exception e)
                {
                    Logger.LogWarning(GetNodeId() + "Encountered error while sending heartbeat to node " + connector.Key + ", request failed with error \"" + e.Message);//+ "\"" + e.StackTrace);
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

        public async void StartElection()
        {
            if (MyUrl != null && CurrentState == NodeState.Candidate)
            {
                Logger.LogInformation(GetNodeId() + "Starting election for term " + (_nodeStorage.CurrentTerm + 1) + ".");
                SetCurrentTerm(_nodeStorage.CurrentTerm + 1);
                var totalVotes = 1;
                //Vote for yourself
                _nodeStorage.SetVotedFor(_nodeStorage.Id);

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
                        Logger.LogWarning(GetNodeId() + "Encountered error while getting vote from node " + connector.Key + ", request failed with error \"" + e.Message + "\"");
                    }
                });

                await Task.WhenAll(tasks);

                if (totalVotes >= _clusterOptions.MinimumNodes)
                {
                    Logger.LogInformation(GetNodeId() + "Recieved enough votes to be promoted, promoting to leader.");
                    SetNodeRole(NodeState.Leader);
                }
                else
                {
                    CurrentLeader = new KeyValuePair<Guid?, string>();
                    _nodeStorage.VotedFor = null;
                }
            }
            else
            {
                Logger.LogWarning(GetNodeId() + "Cannot identify own URL to manage elections...");
            }
        }

        public void SetNodeRole(NodeState newState)
        {
            if (newState == NodeState.Candidate && !_nodeOptions.EnableLeader)
            {
                Logger.LogWarning(GetNodeId() + "Tried to promote to candidate but node has been disabled.");
                return;
            }

            if (newState != CurrentState)
            {
                Console.WriteLine("Node's role changed to " + newState.ToString());
                CurrentState = newState;

                switch (newState)
                {
                    case NodeState.Candidate:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        StopTimer(_clusterInfoTimeoutTimer);
                        InCluster = false;
                        StartElection();
                        break;
                    case NodeState.Follower:
                        //On becoming a follower, wait 5 seconds to allow any other nodes to send out election time outs
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
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
                        ResetTimer(_heartbeatTimer, 0, _clusterOptions.ElectionTimeoutMs / 4);
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
                        Logger.LogWarning(GetNodeId() + "No eligible nodes found, awaiting eligible nodes.");
                        Thread.Sleep(1000);
                        eligbleNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Value.IsContactable).ToDictionary(k => k.Key, v => v.Value);
                    }

                    //var selectedNode = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count()));
                    //Logger.LogDebug(GetNodeId() +"Allocating data shart to node at " + selectedNode.Key);

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
                    Logger.LogDebug(GetNodeId() + "Error while assigning primary node " + e.StackTrace);
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
            Logger.LogInformation(GetNodeId() + "Starting sync of " + shardId + " on node " + MyUrl);
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

            // while (syncs < 2)
            //{
            var shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
            HttpNodeConnector selectedConnector = null;
            while (selectedConnector == null)
            {
                var randomlySelectedNode = shardMetadata.PrimaryAllocation;//shardMetadata.InsyncAllocations[rand.Next(0, shardMetadata.InsyncAllocations.Where(i => i != _nodeStorage.Id).Count())];

                if (_stateMachine.CurrentState.Nodes.ContainsKey(randomlySelectedNode) && NodeConnectors.ContainsKey(_stateMachine.CurrentState.Nodes[randomlySelectedNode].TransportAddress))
                {
                    selectedConnector = NodeConnectors[_stateMachine.CurrentState.Nodes[randomlySelectedNode].TransportAddress];
                }
                else
                {
                    Logger.LogWarning(GetNodeId() + "Node was not found for healing in state, sleeping and waiting again...");
                    Thread.Sleep(1000);
                }
            }
            var latestPrimary = await selectedConnector.Send(new RequestShardOperations()
            {
                From = shard.SyncPos + 1,
                To = shard.SyncPos + 1,
                ShardId = shardId,
                Type = type
            });

            var currentPos = _nodeStorage.GetShardMetadata(shardId).SyncPos;
            var lastPrimaryPosition = latestPrimary.LatestPosition;

            if (currentPos < lastPrimaryPosition)
            {
                while (currentPos < lastPrimaryPosition)
                {
                    Logger.LogInformation(GetNodeId() + "Syncing from " + (shard.SyncPos + 1) + " to " + lastPrimaryPosition);
                    var nextOperations = await selectedConnector.Send(new RequestShardOperations()
                    {
                        From = shard.SyncPos + 1,
                        // do 100 records at a time
                        To = lastPrimaryPosition,
                        ShardId = shardId,
                        Type = type
                    });

                    foreach (var operation in nextOperations.Operations)
                    {
                        Logger.LogInformation(GetNodeId() + "Recovering operation " + operation.Value.Position + " on shard " + shard.ShardId);
                        var result = await Send(new ReplicateShardOperation()
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

                        if (result.LatestPosition > lastPrimaryPosition)
                        {
                            Logger.LogDebug(GetNodeId() + "Updating the sync length to " + lastPrimaryPosition + " for shard " + shard.ShardId);
                            lastPrimaryPosition = result.LatestPosition;
                        }
                    }
                    Logger.LogInformation(GetNodeId() + "Recovery options finished current position " + currentPos + " latest position " + lastPrimaryPosition);
                    // Logger.LogInformation(GetNodeId() + "Synced shard " + shard.ShardId + " upto " + nextOperations.Operations.Last().Key);

                    currentPos = _nodeStorage.GetShardMetadata(shardId).SyncPos;
                    //If there is no next latest position, wait and check after a second
                    if (lastPrimaryPosition == nextOperations.LatestPosition)
                    {
                        Logger.LogInformation(GetNodeId() + "Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
                        shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);

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
                        }

                        while (!_stateMachine.GetShard(type, shardId).InsyncAllocations.Contains(_nodeStorage.Id))
                        {
                            Thread.Sleep(100);
                            Logger.LogInformation(GetNodeId() + "Awaiting the shard " + shardId + " to be insync.");
                        }
                        //Wait three seconds
                        Thread.Sleep(5000);



                        // reset and try to find the next position
                        var tempLastPosition = (await selectedConnector.Send(new RequestShardOperations()
                        {
                            From = shard.SyncPos + 1,
                            To = lastPrimaryPosition,
                            ShardId = shardId,
                            Type = type
                        })).LatestPosition;

                        if (lastPrimaryPosition != tempLastPosition)
                        {
                            lastPrimaryPosition = tempLastPosition;
                            Logger.LogInformation(GetNodeId() + "Detected that shard " + shardMetadata.Id + " has had transient transactions. Continuing to resync.");
                        }
                        else
                        {
                            Logger.LogInformation(GetNodeId() + "Everything on " + shardMetadata.Id + " has been synced and the shard is now insync.");
                        }
                    }
                    else
                    {
                        Logger.LogInformation(GetNodeId() + "Detected shard " + shard.ShardId + " on " + MyUrl + " has a new latest sync position " + lastPrimaryPosition + " < " + nextOperations.LatestPosition);
                        lastPrimaryPosition = nextOperations.LatestPosition;
                    }
                }
            }
            //If the shard is insync but just not marked correctly, set the shard as insync
            else if (shardMetadata.StaleAllocations.Contains(_nodeStorage.Id))
            {
                shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
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
                }
            }

            //Only need to reassign insync node on first operation
            // if (syncs == 0)
            // {


            /*while (!_stateMachine.GetShardMetadata(shard.ShardId, shard.Type).InsyncAllocations.Contains(_nodeStorage.Id))
            {
                Thread.Sleep(1000);
            }*/
            //}
            //  syncs++;
            //}
            Logger.LogInformation(GetNodeId() + "Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
            return true;
        }

        public void SetCurrentTerm(int newTerm)
        {
            //if (newTerm > _nodeStorage.CurrentTerm)
            //{
            SetNodeRole(NodeState.Follower);
            //_nodeStorage.VotedFor = null;
            //CurrentLeader = new KeyValuePair<Guid?, string>();
            _nodeStorage.CurrentTerm = newTerm;
            //}
        }

        public string GetNodeId()
        {
            return "Node:" + _nodeStorage.Id + "(" + MyUrl + "): ";
        }
    }
}