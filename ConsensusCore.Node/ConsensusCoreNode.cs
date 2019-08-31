using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.SystemCommands.Tasks;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services;
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
        where Repository : IBaseRepository
    {
        private Timer _heartbeatTimer;
        private Timer _electionTimeoutTimer;
        private Task _clusterWatchTask;
        private Task _taskWatchTask;
        private Task _indexCreationTask;
        private Task _commitTask;
        private Task _bootstrapTask;
        private Task _nodeSelfHealingTask;
        private Task _shardReassignmentTask;

        private ConcurrentDictionary<Guid, NodeTaskMetadata> _nodeTasks { get; set; } = new ConcurrentDictionary<Guid, NodeTaskMetadata>();

        private NodeOptions _nodeOptions { get; }
        private ClusterOptions _clusterOptions { get; }
        /// <summary>
        /// Allow setting this for testing
        /// </summary>
        public NodeStorage _nodeStorage { get; set; }
        public NodeState CurrentState { get; private set; }
        public ILogger<ConsensusCoreNode<State, Repository>> Logger { get; }

        public Dictionary<string, HttpNodeConnector> NodeConnectors { get; private set; } = new Dictionary<string, HttpNodeConnector>();
        public Dictionary<string, int> NextIndex { get; private set; } = new Dictionary<string, int>();
        public ConcurrentDictionary<string, int> MatchIndex { get; private set; } = new ConcurrentDictionary<string, int>();

        //Used to track whether you are currently already sending logs to a particular node to not double send
        public ConcurrentDictionary<string, bool> LogsSent = new ConcurrentDictionary<string, bool>();
        public IStateMachine<State> _stateMachine { get; private set; }
        public string MyUrl { get; private set; }
        public bool IsBootstrapped = false;
        public KeyValuePair<Guid?, string> CurrentLeader;
        private Thread _findLeaderThread;
        public IDataRouter _dataRouter;
        public bool enableDataRouting = false;
        public bool InCluster { get; private set; } = false;
        ConcurrentQueue<string> IndexCreationQueue { get; set; } = new ConcurrentQueue<string>();
        public bool CompletedFirstLeaderDiscovery = false;
        private Random rand = new Random();

        /// <summary>
        /// Ids of all threads syncing a shard
        /// </summary>
        public ConcurrentDictionary<Guid, Task> SyncThreads = new ConcurrentDictionary<Guid, Task>();

        /// <summary>
        /// What logs have been commited to the state
        /// </summary>
        public int CommitIndex { get; private set; }
        public int LatestLeaderCommit { get; private set; }
        public Repository _repository { get; private set; }
        public string[] NodeUrls;

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
                    ShardSyncPositions = _nodeStorage.GetShardSyncPositions(),
                    ThreadCounts = new
                    {
                        TaskThreads = _nodeTasks?.Where(t => !t.Value.Task.IsCompleted).Count(),
                        IndexCreationThread = !_indexCreationTask?.IsCompleted,
                        CommitThread = !_commitTask?.IsCompleted,
                        BootstrapThread = !_bootstrapTask?.IsCompleted,
                        NodeSelfHealingThread = !_nodeSelfHealingTask?.IsCompleted,
                        ShardAssignmentThread = !_shardReassignmentTask?.IsCompleted,
                        TaskWatch = !_taskWatchTask?.IsCompleted
                    },
                    CurrentRole = CurrentState.ToString(),
                    Term = _nodeStorage.CurrentTerm,
                    LatestLeaderCommit = LatestLeaderCommit
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

        public ConcurrentDictionary<Guid, LocalShardMetaData> LocalShards { get { return _nodeStorage.ShardMetaData; } }

        public bool IsLeader => CurrentLeader.Key.HasValue && CurrentLeader.Key.Value == _nodeStorage.Id;

        public ConsensusCoreNode(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<NodeOptions> nodeOptions,
            ILogger<ConsensusCoreNode<
            State,
            Repository>> logger,
            IStateMachine<State> stateMachine,
            Repository repository,
            IDataRouter dataRouter = null
            )
        {
            Logger = logger;
            _repository = repository;
            var storage = _repository.LoadNodeData();
            if (storage != null)
            {
                Logger.LogInformation("Loaded Consensus Local Node storage from store");
                _nodeStorage = storage;
                _nodeStorage.SetRepository(repository);
            }
            else
            {
                Logger.LogInformation("Failed to load local node storage from store, creating new node storage");
                _nodeStorage = new NodeStorage(repository);
                _nodeStorage.Save();
            }
            _nodeOptions = nodeOptions.Value;
            _clusterOptions = clusterOptions.Value;
            _electionTimeoutTimer = new Timer(ElectionTimeoutEventHandler);
            _heartbeatTimer = new Timer(HeartbeatTimeoutEventHandler);
            _stateMachine = stateMachine;
            SetNodeRole(NodeState.Follower);

            if (!_clusterOptions.TestMode)
            {
                NodeUrls = _clusterOptions.NodeUrls.Split(",");
                _bootstrapTask = Task.Run(() =>
                {
                    //Wait for the rest of the node to bootup
                    Thread.Sleep(3000);
                    BootstrapNode().GetAwaiter().GetResult();
                });
            }
            else
            {
                Console.WriteLine("Running in test mode...");
                IsBootstrapped = true;
                MyUrl = "https://localhost:5022";
                NodeUrls = new string[] { "https://localhost:5022" };
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
            foreach (var url in NodeUrls)
            {
                NextIndex.Add(url, _nodeStorage.GetLogCount() + 1);
                MatchIndex.TryAdd(url, 0);
            }
        }

        public async Task MonitorCommits()
        {
            //Always have this running
            while (true)
            {
                try
                {
                    //As leader, calculate the commit index
                    if (CurrentState == NodeState.Leader)
                    {
                        var indexToAddTo = _nodeStorage.GetLastLogIndex();
                        while (CommitIndex < indexToAddTo)
                        {
                            if (MatchIndex.Values.Count(x => x >= indexToAddTo) >= (_clusterOptions.MinimumNodes - 1))
                            {
                                //You can catch this error as presumably all nodes in cluster wil experience the same error
                                try
                                {
                                    _stateMachine.ApplyLogsToStateMachine(_nodeStorage.Logs.GetRange(CommitIndex, indexToAddTo - CommitIndex));// _nodeStorage.GetLogAtIndex(CommitIndex + 1));
                                }
                                catch (Exception e)
                                {
                                    Logger.LogError(e.Message);
                                }
                                CommitIndex = indexToAddTo;
                                LatestLeaderCommit = indexToAddTo;
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
                                try
                                {
                                    _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                                }
                                catch (Exception e)
                                {
                                    Logger.LogError(e.Message);
                                }
                                CommitIndex = allLogsToBeCommited.Last().Index;
                            }
                            else
                            {
                            }
                        }
                    }
                    //If it was disabled or was a sleep
                    else
                    {
                        Thread.Sleep(1000);
                    }
                    Thread.Sleep(100);
                }
                catch (Exception e)
                {
                    Logger.LogWarning(GetNodeId() + " encountered error " + e.Message + " with stacktrace " + e.StackTrace);
                }
            }
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

        int selfHealingThreads = 0;

        public async Task NodeSelfHealingThread()
        {
            Interlocked.Increment(ref selfHealingThreads);
            while (true)
            {
                if ((CurrentState == NodeState.Follower || CurrentState == NodeState.Leader) && IsUptoDate())
                {
                    try
                    {
                        Logger.LogDebug(GetNodeId() + "Starting self healing." + selfHealingThreads);

                        //If a shard has been unwritten for 10 seconds, send out a poll to see who is upto date.
                        var shardsWhereIAmLeader = _stateMachine.GetShards().Where(sm => sm.PrimaryAllocation == _nodeStorage.Id);
                        var shardChecks = shardsWhereIAmLeader.Select(async shard =>
                        {
                            List<BaseCommand> updates = new List<BaseCommand>();
                            var reloadedShard = _stateMachine.GetShard(shard.Type, shard.Id);
                            //Recheck incase this is now changed.
                            if (reloadedShard.PrimaryAllocation == _nodeStorage.Id)
                            {
                                int? latestPos = _nodeStorage.GetCurrentShardLatestCount(shard.Id);
                                //Allow some time for transactions to be commited on the replica nodes
                                Thread.Sleep(3000);
                                ConcurrentBag<Guid> newStaleAllocations = new ConcurrentBag<Guid>();

                                //For each insync allocation, search whether it is out of date
                                var tasks = reloadedShard.InsyncAllocations.Where(ia => ia != _nodeStorage.Id).Select(async allocation =>
                                    {
                                        try
                                        {
                                            var result = await NodeConnectors[_stateMachine.CurrentState.Nodes[allocation].TransportAddress].Send(new RequestShardOperations()
                                            {
                                                ShardId = shard.Id,
                                                Type = shard.Type,
                                                IncludeOperations = false
                                            });

                                            Logger.LogWarning("My logs are " + latestPos + " and node " + allocation + " has position " + result.LatestPosition);

                                            if (result.LatestPosition != latestPos)
                                            {
                                                var taskId = RecoverShard.GetTaskUniqueId(shard.Id, allocation);
                                                BaseTask recoveryTask = _stateMachine.GetRunningTask(taskId);
                                                if (recoveryTask == null)
                                                {
                                                    if (result.LatestPosition < latestPos)
                                                    {
                                                        Logger.LogInformation(GetNodeId() + " Found a trailing node " + allocation + " for shard " + shard.Id + " remarking as stale.");
                                                        newStaleAllocations.Add(allocation);
                                                    }
                                                    // Reload the shard count to make sure that it is ahead for the wrong reasons
                                                    else if (result.LatestPosition > _nodeStorage.GetCurrentShardLatestCount(shard.Id))
                                                    {
                                                        Logger.LogWarning(GetNodeId() + " found node " + allocation + " for shard " + shard.Id + " as being too far forwards, remarking as stale. " + result.LatestPosition + " vs " + latestPos);
                                                        newStaleAllocations.Add(allocation);
                                                    }
                                                }
                                                else
                                                {
                                                    Logger.LogInformation(GetNodeId() + "Skipping marking node " + allocation + " as stale for shard " + shard.Id + " as there is still a task (" + recoveryTask.Id + ") running.");
                                                }
                                            }
                                        }
                                        catch (TaskCanceledException e)
                                        {
                                            Logger.LogError(GetNodeId() + " failed to find the latest positions for shard " + shard.Id + " for allocation " + allocation + " request to " + _stateMachine.CurrentState.Nodes[allocation].TransportAddress + " timed out.");
                                        }
                                        catch (Exception e)
                                        {
                                            Logger.LogError(GetNodeId() + " failed to find the latest positions for shard " + shard.Id + " for allocation " + allocation + " with error " + e.Message + Environment.NewLine + e.StackTrace);
                                        }
                                    });

                                await Task.WhenAll(tasks);

                                //Recheck that I am still the primary
                                if ((newStaleAllocations.Count > 0 || latestPos != reloadedShard.LatestOperationPos) && _stateMachine.GetShard(shard.Type, shard.Id).PrimaryAllocation == _nodeStorage.Id)
                                {
                                    updates.Add(new UpdateShardMetadataAllocations()
                                    {
                                        ShardId = shard.Id,
                                        Type = shard.Type,
                                        StaleAllocationsToAdd = newStaleAllocations.ToHashSet(),
                                        InsyncAllocationsToRemove = newStaleAllocations.ToHashSet(),
                                        LatestPos = latestPos,
                                        DebugLog = "Primary node " + _nodeStorage.Id + " found these nodes to be not upto date."
                                    });
                                }
                                if (updates.Count > 0)
                                {
                                    await Send(new ExecuteCommands()
                                    {
                                        Commands = updates,
                                        WaitForCommits = true
                                    });
                                }
                            }
                        });

                        await Task.WhenAll(shardChecks);

                        //Create a task if shards are stale and a recovery task is not running
                        var shardsWhereIAmStale = _stateMachine.GetShards().Where(sm => sm.StaleAllocations.Contains(_nodeStorage.Id));

                        List<BaseTask> shardCommands = new List<BaseTask>();
                        foreach (var shard in shardsWhereIAmStale)
                        {
                            var taskId = RecoverShard.GetTaskUniqueId(shard.Id, _nodeStorage.Id);
                            BaseTask recoveryTask = _stateMachine.GetRunningTask(taskId);
                            if (recoveryTask == null)
                            {
                                Logger.LogInformation(GetNodeId() + "Found I have a stale version of " + shard.Id + " and I am not recovering. Adding a recovery task");
                                shardCommands.Add(new RecoverShard()
                                {
                                    Id = Guid.NewGuid(),
                                    ShardId = shard.Id,
                                    NodeId = _nodeStorage.Id,
                                    Type = shard.Type,
                                    UniqueRunningId = taskId,
                                    CreatedOn = DateTime.UtcNow
                                });
                            }
                            else
                            {
                                Logger.LogInformation(GetNodeId() + "Found I have a stale version of " + shard.Id + " i am already recovering using task " + recoveryTask + " with status " + recoveryTask.Status.ToString() + ".");
                            }
                        }

                        if (shardCommands.Count > 0)
                        {
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>() {
                           new UpdateClusterTasks()
                                {
                                    TasksToAdd = shardCommands
                                }
                            },
                                WaitForCommits = true
                            });
                        }

                        //Start checking for orphaned tasks
                        var runningNodeTasks = _nodeTasks.Select(nt => nt.Key).ToList();
                        //Get all node tasks that are technically in progress but not running on the node, most likely a interuption in execution because of a restart.
                        var invalidTasks = _stateMachine.CurrentState.GetClusterTasks(new ClusterTaskStatuses[] {
                        ClusterTaskStatuses.InProgress
                    }, _nodeStorage.Id).Where(ct => !runningNodeTasks.Contains(ct));

                        if (invalidTasks.Count() > 0)
                        {
                            Logger.LogInformation(GetNodeId() + "Found a number of tasks (" + invalidTasks.Count() + ") that are now not running in memory but marked for inprogress. Erroring the tasks " + JsonConvert.SerializeObject(invalidTasks, Formatting.Indented));
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>()
                            {
                                new UpdateClusterTasks()
                                {
                                    TasksToUpdate = invalidTasks.Select(it =>
                                        new TaskUpdate()
                                        {
                                            Status = ClusterTaskStatuses.Error,
                                            CompletedOn = DateTime.UtcNow,
                                            TaskId = it,
                                            ErrorMessage = "Task is not found to be running on node. Potential restart after the task had started."
                                        }
                                    ).ToList()
                                }
                            },
                                WaitForCommits = true
                            });
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(GetNodeId() + "Encountered error while self-healing" + e.StackTrace);
                    }

                    Thread.Sleep(1000);
                }
                else if (!IsUptoDate())
                {
                    Logger.LogWarning("Awaiting for not to commit all logs..");
                    Thread.Sleep(1000);
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }

        public async Task<bool> BootstrapNode()
        {
            Logger.LogInformation(GetNodeId() + "Bootstrapping Node!");

            // The node cannot bootstrap unless at least a majority of nodes are present
            NodeConnectors.Clear();
            while (MyUrl == null)
            {
                foreach (var url in NodeUrls)
                {
                    var testConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(_clusterOptions.DataTransferTimeoutMs));
                    //Always add the connector
                    if (!NodeConnectors.ContainsKey(url))
                    {
                        NodeConnectors.Add(url, testConnector);
                    }

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
            }

            IsBootstrapped = true;
            return true;
        }

        private async Task StartShardReassignment()
        {
            while (CurrentState == NodeState.Leader)
            {
                foreach (var shard in _stateMachine.GetShards())
                {
                    List<RecoverShard> newRecoveryTasks = new List<RecoverShard>();

                    //Add the node to stale shards to start syncing
                    HashSet<Guid> newStaleAllocations = new HashSet<Guid>();
                    int newStaleNodeCount = 0;
                    foreach (var node in _stateMachine.GetNodes())
                    {
                        //It is not stale or insync
                        if (!shard.InsyncAllocations.Contains(node.Id) && !shard.StaleAllocations.Contains(node.Id))
                        {
                            newStaleNodeCount++;
                            newStaleAllocations.Add(node.Id);
                        }
                    }

                    if (newStaleNodeCount > 0)
                    {
                        var shardInfo = _stateMachine.GetShard(shard.Type, shard.Id);
                        HashSet<Guid> staleNodes = new HashSet<Guid>();
                        /*foreach (var allo in shardInfo.StaleAllocations)
                        {
                            staleNodes.Add(allo);
                        }*/
                        // Only add the ones that are not already marked as stale
                        foreach (var newStaleNode in newStaleAllocations.Where(a => !staleNodes.Contains(a)))
                            staleNodes.Add(newStaleNode);


                        // Create a recovery task of all stale allocations
                        foreach (var staleAllocation in newStaleAllocations)
                        {
                            var taskId = "Recover_Shard_" + shard.Id + "_" + staleAllocation;

                            Logger.LogDebug("Detected stale allocation for " + shard.Id + " on node " + staleAllocation);
                            //There is no current running task with the same issue
                            if (_stateMachine.CurrentState.ClusterTasks.Where(ct => ct.Value.UniqueRunningId == taskId && ct.Value.CompletedOn == null).Count() == 0)
                            {
                                newRecoveryTasks.Add(new RecoverShard()
                                {
                                    Id = Guid.NewGuid(),
                                    ShardId = shard.Id,
                                    NodeId = staleAllocation,
                                    Type = shard.Type,
                                    UniqueRunningId = taskId,
                                    CreatedOn = DateTime.UtcNow
                                });
                            }
                            else
                            {
                                Logger.LogDebug("Detected existing task allocation for " + shard.Id + " on node " + staleAllocation + JsonConvert.SerializeObject(_stateMachine.CurrentState.ClusterTasks.Where(ct => ct.Value.UniqueRunningId == taskId && ct.Value.CompletedOn == null), Formatting.Indented));
                            }
                        }

                        if (staleNodes.Count() > 0)
                        {
                            //Add nodes that were previously not in the cluster to automatically be assigned as a stale node
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>() {
                                new UpdateShardMetadataAllocations()
                                {
                                    ShardId = shard.Id,
                                    StaleAllocationsToAdd = staleNodes,
                                    Type = shardInfo.Type
                                },
                            },
                                WaitForCommits = true
                            });
                            Logger.LogInformation(GetNodeId() + "Added " + newStaleAllocations.Count() + " nodes to stale allocations in" + shard.Id);
                        }

                    }
                }


                Thread.Sleep(3000);
            }
        }

        private async Task StartIndexCreation()
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

                            DateTime startTime = DateTime.Now;
                            while (!_stateMachine.IndexExists(typeToCreate))
                            {
                                if ((DateTime.Now - startTime).TotalMilliseconds < _clusterOptions.DataTransferTimeoutMs)
                                {
                                    throw new Exception("Failed to create index " + typeToCreate + ", timed out index detection.");
                                }
                                Logger.LogDebug(GetNodeId() + "Awaiting index creation.");
                                Thread.Sleep(100);
                            }
                        }
                        else
                        {
                            Logger.LogDebug(GetNodeId() + "INDEX for type " + typeToCreate + " Already exists, skipping creation...");
                        }
                    }
                }
                while (isSuccessful);
                Thread.Sleep(1000);
            }
        }

        #region Timeout Handlers
        public async Task StartClusterWatchTask()
        {
            while (CurrentState == NodeState.Leader)
            {
                try
                {
                    var nodeUpsertCommands = new List<BaseCommand>();
                    if (!_clusterOptions.TestMode)
                    {
                        Logger.LogDebug(GetNodeId() + "Rediscovering nodes...");
                        ConcurrentBag<Guid> NodesToMarkAsStale = new ConcurrentBag<Guid>();
                        ConcurrentBag<Guid> NodesToRemove = new ConcurrentBag<Guid>();
                        var nodeUrlTasks = NodeUrls.Select(async url =>
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
                                if ((nodeId.Value != null && (!_stateMachine.CurrentState.Nodes.ContainsKey(nodeId.Value) ||
                                    // Check whether the node with the same id has different information
                                    !_stateMachine.CurrentState.Nodes[nodeId.Value].Equals(possibleNodeUpdate)
                                    //Node was uncontactable now its contactable
                                    || !_stateMachine.IsNodeContactable(nodeId.Value))
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
                                    if (conflictingNodes.Count() > 0)
                                    {
                                        var conflictingNodeId = conflictingNodes.First().Key;
                                        Logger.LogWarning(GetNodeId() + "Detected another node with conflicting transport address, removing the conflicting node from the cluster");
                                        nodeUpsertCommands.Add(new DeleteNodeInformation()
                                        {
                                            Id = conflictingNodes.First().Key
                                        });
                                        NodesToRemove.Add(conflictingNodeId);
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.LogWarning(GetNodeId() + "Node at url " + url + " was unreachable...");
                                var staleNode = _stateMachine.GetNode(url);
                                if (staleNode != null)
                                    NodesToMarkAsStale.Add(staleNode.Id);
                            }
                        });

                        await Task.WhenAll(nodeUrlTasks);
                        var rand = new Random();

                        if ((NodesToMarkAsStale.Count() > 0 || NodesToRemove.Count() > 0) && _clusterOptions.MinimumNodes > 1)
                        {
                            Logger.LogWarning(GetNodeId() + "Found stale or removed nodes, reassigning all nodes");

                            foreach (var shard in _stateMachine.GetShards())
                            {
                                if (NodesToMarkAsStale.Contains(shard.PrimaryAllocation) || shard.InsyncAllocations.Where(i => NodesToRemove.Contains(i) || NodesToMarkAsStale.Contains(i)).Count() > 0 || shard.StaleAllocations.Where(i => NodesToRemove.Contains(i)).Count() > 0)
                                {
                                    Logger.LogDebug(GetNodeId() + "Reassigned shard " + shard.Id);
                                    /*var realInsyncShards = shard.InsyncAllocations.Where(id => !NodesToRemove.Contains(id) && !NodesToMarkAsStale.Contains(id)).ToHashSet<Guid>();
                                    var realStaleShards = shard.StaleAllocations.Where(id => !NodesToRemove.Contains(id)).ToList();
                                    realStaleShards.AddRange(NodesToMarkAsStale);
                                    */
                                    var invalidInsyncAllocations = shard.InsyncAllocations.Where(ia => NodesToRemove.Contains(ia) || NodesToMarkAsStale.Contains(ia));
                                    //Get node with the highest shard
                                    var insyncAllocations = shard.InsyncAllocations.Where(ia => !invalidInsyncAllocations.Contains(ia));
                                    Logger.LogInformation("NEW INVALID: " + JsonConvert.SerializeObject(invalidInsyncAllocations));
                                    Logger.LogInformation("INSYNC: " + JsonConvert.SerializeObject(shard.InsyncAllocations));

                                    Guid newPrimary = !invalidInsyncAllocations.Contains(shard.PrimaryAllocation) ? shard.PrimaryAllocation : insyncAllocations.ElementAt(rand.Next(0, insyncAllocations.Count()));// await GetMostUpdatedNode(shard.Id, shard.Type, shard.InsyncAllocations.Where(ia => !invalidInsyncAllocations.Contains(ia)).ToArray());
                                    nodeUpsertCommands.Add(new UpdateShardMetadataAllocations()
                                    {
                                        PrimaryAllocation = newPrimary,
                                        InsyncAllocationsToRemove = invalidInsyncAllocations.ToHashSet<Guid>(),
                                        StaleAllocationsToAdd = NodesToMarkAsStale.ToHashSet<Guid>(),
                                        StaleAllocationsToRemove = NodesToRemove.ToHashSet<Guid>(),
                                        ShardId = shard.Id,
                                        Type = shard.Type,
                                        DebugLog = GetNodeId() + "Reassigning nodes based on node becoming unavailable. Primary node is " + newPrimary
                                    });
                                    // }
                                    /*else
                                    {
                                        Logger.LogError("No live primary node available for shard " + shard.Id);
                                        nodeUpsertCommands.Add(new UpdateShardMetadata()
                                        {
                                            PrimaryAllocation = realInsyncShards.ElementAt(rand.Next(0, realInsyncShards.Count)),
                                            InsyncAllocations = realInsyncShards,
                                            StaleAllocations = realStaleShards.ToHashSet<Guid>(),
                                            ShardId = shard.Id,
                                            Type = shard.Type
                                        });
                                    }*/

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
                catch (Exception e)
                {
                    Logger.LogError("Failed to run Cluster Info Timeout Handler with error " + e.StackTrace);
                }
            }
        }

        /// <summary>
        /// Get the node with the highest shard sync position
        /// </summary>
        /// <returns></returns>
        public async Task<Guid?> GetMostUpdatedNode(Guid shardId, string type, Guid[] nodeIds)
        {
            ConcurrentDictionary<Guid, int> syncPositions = new ConcurrentDictionary<Guid, int>();
            var tasks = _stateMachine.CurrentState.Nodes.Where(nc => nodeIds.Contains(nc.Key)).Select(async shard =>
            {
                try
                {
                    var shardResult = await NodeConnectors[shard.Value.TransportAddress].Send(new RequestShardOperations()
                    {
                        IncludeOperations = false,
                        ShardId = shardId,
                        Type = type
                    });

                    var result = syncPositions.TryAdd(shard.Key, shardResult.LatestPosition);
                    if (!result)
                    {
                        Logger.LogError("Concurrency issues with adding the result");
                    }
                }
                catch (Exception e)
                {
                    Logger.LogError("Failed to get shard(" + shardId + ") information from " + shard.Key + " with exception " + e.Message + " stack trace: " + e.StackTrace);
                }

            });

            await Task.WhenAll(tasks);
            Logger.LogInformation("Checked " + tasks.Count() + " with " + syncPositions.Count() + " results");
            Guid? latestNode = null;
            int latestPrimary = 0;

            foreach (var pos in syncPositions)
            {
                if (pos.Value > latestPrimary || latestNode == null)
                {
                    latestNode = pos.Key;
                }
            }
            return latestNode;
        }

        public async Task GetTaskWatch()
        {
            while (CurrentState == NodeState.Follower || CurrentState == NodeState.Leader)
            {
                if (IsBootstrapped)
                {
                    Logger.LogDebug(GetNodeId() + "Starting task watch.");
                    //Check tasks assigned to this node
                    var tasks = _stateMachine.CurrentState.ClusterTasks.Where(t => t.Value.CompletedOn == null && t.Value.Status != ClusterTaskStatuses.InProgress && t.Value.NodeId == _nodeStorage.Id).Select(s => s.Value).ToList();
                    var currentTasksNo = _nodeTasks.Where(t => !t.Value.Task.IsCompleted).Count();
                    var numberOfTasksToAssign = (tasks.Count() > (_clusterOptions.ConcurrentTasks - currentTasksNo)) ? (_clusterOptions.ConcurrentTasks - currentTasksNo) : tasks.Count();

                    Logger.LogDebug(GetNodeId() + numberOfTasksToAssign + "tasks to run. || " + currentTasksNo);
                    if (numberOfTasksToAssign > 0)
                    {
                        await Send(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>()
                                {
                                    new UpdateClusterTasks()
                                    {
                                        TasksToUpdate = tasks.GetRange(0, numberOfTasksToAssign).Select(t => new TaskUpdate(){
                                              Status = ClusterTaskStatuses.InProgress,
                                              CompletedOn = DateTime.UtcNow,
                                              TaskId = t.Id
                                        }).ToList()
                                    }
                                },
                            WaitForCommits = true
                        });

                        //Create a thread for each task
                        for (var i = 0; i < numberOfTasksToAssign; i++)
                        {
                            Logger.LogDebug(GetNodeId() + " is starting task " + tasks[i].ToString());
                            try
                            {
                                var newTask = StartNodeTask(tasks[i]);
                                _nodeTasks.TryAdd(tasks[i].Id
                                    , new NodeTaskMetadata()
                                    {
                                        Id = tasks[i].Id,
                                        Task = Task.Run(() => newTask)
                                    });
                            }
                            catch (Exception e)
                            {
                                Logger.LogCritical(GetNodeId() + "Failed to fail step " + tasks[i].Id + " gracefully.");
                            }
                        }
                    }
                }
                Thread.Sleep(1000);
            }
        }

        public async Task StartNodeTask(BaseTask task)
        {
            try
            {
                Logger.LogDebug(GetNodeId() + "Starting task " + task.Id);
                switch (task)
                {
                    case RecoverShard t:
                        await SyncShard(t.ShardId, t.Type);
                        await Send(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>()
                                {
                                    new UpdateClusterTasks()
                                    {
                                        TasksToUpdate = new List<TaskUpdate>()
                                        {
                                            new TaskUpdate()
                                            {
                                                Status = ClusterTaskStatuses.Successful,
                                                CompletedOn = DateTime.UtcNow,
                                            TaskId = task.Id
                                            }
                                        }
                                    }
                                },
                            WaitForCommits = true
                        });
                        break;
                }
            }
            catch (Exception e)
            {
                Logger.LogError(GetNodeId() + "Failed to complete task " + task.Id + " with error " + e.Message + Environment.NewLine + e.StackTrace);
                await Send(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                            {
                                new UpdateClusterTasks()
                                {
                                    TasksToUpdate = new List<TaskUpdate>()
                                    {
                                        new TaskUpdate()
                                        {
                                            Status = ClusterTaskStatuses.Error,
                                            CompletedOn = DateTime.UtcNow,
                                            TaskId = task.Id,
                                            ErrorMessage = e.Message + ": " +  e.StackTrace

                                        }
                                    }
                                }
                            },
                    WaitForCommits = true
                });
            }
        }

        public async void ElectionTimeoutEventHandler(object args)
        {
            SetNodeRole(NodeState.Candidate);
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
        public async Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {
            try
            {

                Logger.LogDebug(GetNodeId() + "Detected RPC " + request.GetType().Name + ".");
                if (!IsBootstrapped)
                {
                    Logger.LogDebug(GetNodeId() + "Node is not ready...");
                    return new TResponse()
                    {
                        IsSuccessful = false
                    };
                }

                if (IsClusterRequest<TResponse>(request) && !InCluster)
                {
                    Logger.LogWarning(GetNodeId() + "Reqeuest rejected, node is not apart of cluster...");
                    return new TResponse()
                    {
                        IsSuccessful = false
                    };
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
                    case RequestCreateIndex t1:
                        response = await HandleIfLeaderOrReroute(request, () => (TResponse)(object)(CreateIndexHandler(t1)));
                        break;
                    case RequestInitializeNewShard t1:
                        response = (TResponse)(object)RequestInitializeNewShardHandler(t1);
                        break;
                    case ReplicateShardOperation t1:
                        response = (TResponse)(object)await ReplicateShardOperationHandler(t1);
                        break;
                    case RequestShardOperations t1:
                        response = (TResponse)(object)await RequestShardOperationsHandler(t1);
                        break;
                    default:
                        throw new Exception("Request is not implemented");
                }
                return response;
            }
            catch (TaskCanceledException e)
            {
                Logger.LogWarning(GetNodeId() + "Request " + request.RequestName + " timed out...");
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
            catch (Exception e)
            {
                Logger.LogError(GetNodeId() + "Failed request " + request.RequestName + " with error " + e.StackTrace);
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
        }

        public bool IsClusterRequest<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse
        {
            switch (request)
            {
                case ExecuteCommands t1:
                    return true;
                case RequestDataShard t1:
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

        public async Task<RequestShardOperationsResponse> RequestShardOperationsHandler(RequestShardOperations request)
        {
            //Check that the shard is insync here
            var localShard = _nodeStorage.GetShardMetadata(request.ShardId);

            if (localShard == null)
            {
                Logger.LogError(GetNodeId() + "Request for shard " + request.ShardId + " however shard does not exist on node.");
                return new RequestShardOperationsResponse()
                {
                    IsSuccessful = false
                };
            }
            SortedDictionary<int, ShardOperationMessage> FinalList = new SortedDictionary<int, ShardOperationMessage>();


            if (request.IncludeOperations)
            {
                //foreach value in from - to, pull the operation and then pull the object from object router
                for (var i = request.From; i <= request.To; i++)
                {
                    var operation = _nodeStorage.GetOperation(request.ShardId, i);
                    if (operation != null)
                    {
                        //This data could be in a future state
                        var currentData = (ShardData)await _dataRouter.GetDataAsync(request.Type, operation.ObjectId);
                        FinalList.Add(i, new ShardOperationMessage()
                        {
                            ObjectId = operation.ObjectId,
                            Operation = operation.Operation,
                            Payload = currentData,
                            Position = i
                        });
                    }
                }
            }

            return new RequestShardOperationsResponse()
            {
                IsSuccessful = true,
                LatestPosition = _nodeStorage.GetCurrentShardLatestCount(request.ShardId),
                Operations = FinalList
            };
        }

        public async Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<TResponse> Handle) where TResponse : BaseResponse, new()
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
                        return new TResponse()
                        {
                            IsSuccessful = false
                        };
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
                        Logger.LogError(GetNodeId() + "Encountered " + e.Message + " while trying to route " + request.GetType().Name + " to leader.");
                        return new TResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                }
            }
            return Handle();
        }

        public async Task<ReplicateShardOperationResponse> ReplicateShardOperationHandler(ReplicateShardOperation request)
        {
            try
            {
                Logger.LogDebug(GetNodeId() + "Recieved replication request for shard" + request.ShardId + " for object " + request.Operation.ObjectId + " for action " + request.Operation.Operation.ToString() + " operation " + request.Pos);
                var startTime = DateTime.Now;
                while (!_nodeStorage.CanApplyOperation(request.ShardId, request.Pos))
                {
                    Thread.Sleep(100);
                }

                if (_nodeStorage.ReplicateShardOperation(request.ShardId, request.Pos, request.Operation))
                {
                    if (!await RunDataOperation(request.Operation.Operation, request.Payload))
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
                        _nodeStorage.MarkOperationAsCommited(request.ShardId, request.Pos);
                        Logger.LogDebug(GetNodeId() + "Marked operation " + request.Pos + " on shard " + request.ShardId + "as commited");
                    }
                }
                Logger.LogDebug(GetNodeId() + "Successfully replicated request for shard" + request.ShardId + " for object " + request.Operation.ObjectId + " for action " + request.Operation.Operation.ToString());
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
            int index = _nodeStorage.AddCommands(request.Commands.ToList(), _nodeStorage.CurrentTerm);
            var startDate = DateTime.Now;
            while (request.WaitForCommits)
            {
                if ((DateTime.Now - startDate).TotalMilliseconds > _clusterOptions.CommitsTimeout)
                {
                    return new ExecuteCommandsResponse()
                    {
                        EntryNo = index,
                        IsSuccessful = false
                    };
                }

                Logger.LogDebug(GetNodeId() + "Waiting for " + request.RequestName + " to complete.");
                if (CommitIndex >= index)
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

        public async Task<bool> RunDataOperation(ShardOperationOptions operation, ShardData shard)
        {
            try
            {
                switch (operation)
                {
                    case ShardOperationOptions.Create:
                        await _dataRouter.InsertDataAsync(shard);
                        break;
                    case ShardOperationOptions.Update:
                        if (!_nodeStorage.IsObjectMarkedForDeletion(shard.ShardId.Value, shard.Id))
                        {
                            try
                            {
                                await _dataRouter.UpdateDataAsync(shard);
                            }
                            catch (Exception e)
                            {
                                Logger.LogWarning(GetNodeId() + "Failed to update data with exception " + e.Message + " trying to add the data instead.");
                                await _dataRouter.InsertDataAsync(shard);
                            }
                        }
                        else
                        {
                            Console.WriteLine("OBJECT IS MARKED FOR DELETION");
                        }
                        break;
                    case ShardOperationOptions.Delete:
                        if (_nodeStorage.MarkShardForDeletion(shard.ShardId.Value, shard.Id))
                        {
                            await _dataRouter.DeleteDataAsync(shard);
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
                Logger.LogError("Failed to run data operation against shard " + shard.Id + " with exception " + e.StackTrace);
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

        Dictionary<Guid, object> Locks = new Dictionary<Guid, object>();

        /// <summary>
        /// Initial update should occur on the primary node which will then replicate it to the rest of the cluster.
        /// Note, the request will fail if the primary goes down during the request
        /// </summary>
        /// <param name="shard"></param>
        /// <returns></returns>
        public async Task<WriteDataResponse> WriteDataRPCHandler(WriteData shard)
        {
            Logger.LogDebug(GetNodeId() + "Received write request for object " + shard.Data.Id + " for shard " + shard.Data.ShardId);
            //Check if index exists, if not - create one
            if (!_stateMachine.IndexExists(shard.Data.ShardType))
            {
                await Send(new RequestCreateIndex()
                {
                    Type = shard.Data.ShardType
                });

                DateTime startIndexCreation = DateTime.Now;
                while (!_stateMachine.IndexExists(shard.Data.ShardType))
                {
                    if ((DateTime.Now - startIndexCreation).Milliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        throw new IndexCreationFailedException("Index creation for shard " + shard.Data.ShardType + " timed out.");
                    }
                    Thread.Sleep(100);
                }
            }

            SharedShardMetadata shardMetadata;

            if (shard.Data.ShardId == null)
            {
                var allocations = _stateMachine.GetShards(shard.Data.ShardType);
                Random rand = new Random();
                var selectedNodeIndex = rand.Next(0, allocations.Length);
                shard.Data.ShardId = allocations[selectedNodeIndex].Id;
                shardMetadata = allocations[selectedNodeIndex];
            }
            else
            {
                shardMetadata = _stateMachine.GetShard(shard.Data.ShardType, shard.Data.ShardId.Value);
            }

            //If the shard is assigned to you
            if (shardMetadata.PrimaryAllocation == _nodeStorage.Id)
            {
                if (_nodeStorage.GetShardMetadata(shard.Data.ShardId.Value) == null)
                {
                    Logger.LogDebug(GetNodeId() + "Creating local copy of shard " + shard.Data.ShardId);
                    _nodeStorage.AddNewShardMetaData(new LocalShardMetaData()
                    {
                        ShardId = shard.Data.ShardId.Value,
                        ShardOperations = new ConcurrentDictionary<int, ShardOperation>(),
                        Type = shard.Data.ShardType
                    });
                }

                //Commit the sequence Number
                int sequenceNumber = _nodeStorage.AddNewShardOperation(shard.Data.ShardId.Value, new ShardOperation()
                {
                    ObjectId = shard.Data.Id,
                    Operation = shard.Operation
                });



                if (!await RunDataOperation(shard.Operation, shard.Data))
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
                    //If the shard metadata is not synced upto date
                    if (_nodeStorage.GetShardMetadata(shard.Data.ShardId.Value).SyncPos < sequenceNumber - 1)
                    {
                        //Logger.LogInformation(GetNodeId() + "Detected delayed sync position, sending recovery command.");
                        //AddShardSyncTask(shard.Data.Id, shard.Data.ShardType);
                    }
                    _nodeStorage.MarkOperationAsCommited(shard.Data.ShardId.Value, sequenceNumber);

                    //Write to the replicated nodes
                    ConcurrentBag<Guid> InvalidNodes = new ConcurrentBag<Guid>();

                    bool successfullyMarkedOutOfSync = false;
                    //All allocations except for your own
                    var tasks = shardMetadata.InsyncAllocations.Where(id => id != _nodeStorage.Id).Select(async allocation =>
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
                                 Type = shard.Data.ShardType
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
                         catch (TaskCanceledException e)
                         {
                             Logger.LogError(GetNodeId() + "Failed to replicate shard " + shardMetadata.Id + " on shard " + _stateMachine.CurrentState.Nodes[allocation].TransportAddress + " for operation " + sequenceNumber + " as request timed out, marking shard as not insync...");
                             InvalidNodes.Add(allocation);
                         }
                         catch (Exception e)
                         {
                             Logger.LogError(GetNodeId() + "Failed to replicate shard " + shardMetadata.Id + " for operation " + sequenceNumber + ", marking shard as not insync..." + e.StackTrace);
                             InvalidNodes.Add(allocation);
                         }
                     });

                    await Task.WhenAll(tasks);

                    if (InvalidNodes.Count() > 0)
                    {
                        Logger.LogWarning(GetNodeId() + "Detected invalid nodes, setting nodes " + InvalidNodes.Select(ivn => ivn.ToString()).Aggregate((i, j) => i + "," + j) + " to be out-of-sync");

                        var result = await Send(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>()
                        {
                            new UpdateShardMetadataAllocations()
                            {
                                ShardId = shardMetadata.Id,
                                Type = shardMetadata.Type,
                                InsyncAllocationsToRemove = InvalidNodes.ToHashSet(),
                                StaleAllocationsToAdd = InvalidNodes.ToHashSet()
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
                }
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
            }

            if (shard.RemoveLock)
            {
                var result = await Send(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                        {
                            new RemoveObjectLock()
                            {
                                ObjectId = shard.Data.Id,
                                Type = shardMetadata.Type
                            }
                },
                    WaitForCommits = true
                });
            }

            return new WriteDataResponse()
            {
                IsSuccessful = true,
                ShardId = shard.Data.ShardId.Value
            };
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
                        Logger.LogDebug(GetNodeId() + "Voting for " + requestVoteRPC.CandidateId + " for term " + requestVoteRPC.Term);
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        SetCurrentTerm(requestVoteRPC.Term);
                        successful = true;
                    }
                    else if (_nodeStorage.CurrentTerm > requestVoteRPC.Term)
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as current term is greater (" + requestVoteRPC.Term + "<" + _nodeStorage.CurrentTerm + ") | " + CurrentState.ToString());
                    }
                    else if (requestVoteRPC.LastLogIndex < _nodeStorage.GetLogCount() - 1)
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as last log index is less then local index (" + requestVoteRPC.LastLogIndex + "<" + (_nodeStorage.GetLogCount() - 1) + ")" + CurrentState.ToString());
                    }
                    else if (requestVoteRPC.LastLogTerm < _nodeStorage.GetLastLogTerm())
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as last log term is less then local term (" + requestVoteRPC.LastLogTerm + "<" + _nodeStorage.GetLastLogTerm() + ")" + CurrentState.ToString());
                    }
                    else if ((_nodeStorage.VotedFor != null && _nodeStorage.VotedFor != requestVoteRPC.CandidateId))
                    {
                        Logger.LogDebug(GetNodeId() + "Rejected vote from " + requestVoteRPC.CandidateId + " as I have already voted for " + _nodeStorage.VotedFor + " | " + CurrentState.ToString());
                    }
                    else if (!successful)
                    {
                        Logger.LogError("Rejected vote from " + requestVoteRPC.CandidateId + " due to unknown reason.");
                    }
                }
            }
            return new RequestVoteResponse()
            {
                IsSuccessful = successful
            };
        }

        public AppendEntryResponse AppendEntryRPCHandler(AppendEntry entry)
        {
            //Check the log check to prevent a intermittent term increase with no back tracking, TODO check whether this causes potentially concurrency issues
            if (entry.Term < _nodeStorage.CurrentTerm && entry.LeaderCommit <= CommitIndex)
            {
                Logger.LogDebug(GetNodeId() + "Rejected RPC from " + entry.LeaderId + " due to lower term " + entry.Term + "<" + _nodeStorage.CurrentTerm);
                return new AppendEntryResponse()
                {
                    ConflictName = "Old Term Name",
                    IsSuccessful = false
                };
            }

            //Reset the timer if the append is from a valid term
            ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);

            //If you are a leader or candidate, swap to a follower
            if (CurrentState == NodeState.Candidate || CurrentState == NodeState.Leader)
            {
                Logger.LogWarning(GetNodeId() + " detected node " + entry.LeaderId + " is further ahead. Changing to follower");
                SetNodeRole(NodeState.Follower);
            }

            if (CurrentLeader.Key != entry.LeaderId)
            {
                Logger.LogDebug(GetNodeId() + "Detected uncontacted leader, discovering leader now.");
                //Reset the current leader
                CurrentLeader = new KeyValuePair<Guid?, string>(entry.LeaderId, null);
                _findLeaderThread = FindLeaderThread(entry.LeaderId);
                _findLeaderThread.Start();
            }

            if (entry.LeaderCommit > LatestLeaderCommit)
            {
                Logger.LogDebug(GetNodeId() + "Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                LatestLeaderCommit = entry.LeaderCommit;
            }

            var previousEntry = _nodeStorage.GetLogAtIndex(entry.PrevLogIndex);

            if (previousEntry == null && entry.PrevLogIndex != 0)
            {
                Logger.LogWarning(GetNodeId() + "Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");

                return new AppendEntryResponse()
                {
                    IsSuccessful = false,
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
                    IsSuccessful = false,
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


            if (!InCluster)
            {
                InCluster = true;
            }

            DateTime time = DateTime.Now;

            foreach (var log in entry.Entries)
            {
                _nodeStorage.AddLog(log);
            }

            return new AppendEntryResponse()
            {
                IsSuccessful = true
            };
        }

        public async Task<RequestDataShardResponse> RequestDataShardHandler(RequestDataShard request)
        {
            bool found = false;
            RequestDataShardResponse data = null;
            var currentTime = DateTime.Now;
            Guid? FoundShard = null;
            Guid? FoundOnNode = null;
            bool appliedLock = false;
            Guid? lockId = null;

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
                            Guid newLockId = Guid.NewGuid();
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>{
                                    new SetObjectLock()
                                    {
                                        ObjectId = request.ObjectId,
                                        Type = request.Type,
                                        LockId = newLockId
                                    }
                                },
                                WaitForCommits = true
                            });

                            var lockStartTime = DateTime.Now;
                            //While the object is not locked yet, wait
                            while (!_stateMachine.IsObjectLocked(request.ObjectId))
                            {
                                if ((DateTime.Now - lockStartTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                                {
                                    throw new ClusterOperationTimeoutException("Locking operation timed out on " + request.ObjectId);
                                }
                                Thread.Sleep(100);
                            }

                            //After the objects been locked check the ID
                            if (!_stateMachine.IsLockObtained(request.ObjectId, newLockId))
                            {
                                throw new ConflictingObjectLockException("Object was locked by another process...");
                            }

                            lockId = newLockId;
                            appliedLock = true;
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
                            AppliedLocked = false,
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
                        finalObject = await _dataRouter.GetDataAsync(request.Type, request.ObjectId);
                        foundResult = finalObject != null ? true : false;
                        FoundShard = shard.Key;
                        FoundShard = shard.Value;
                        Interlocked.Increment(ref totalRespondedShards);
                    }
                });

                //Don't await, this will trigger the tasks
                Task.WhenAll(tasks);

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
                    SearchMessage = finalObject != null ? null : "Object " + request.ObjectId + " could not be found in shards.",
                    LockId = lockId,
                    AppliedLocked = appliedLock

                };
            }
            else
            {
                var finalObject = await _dataRouter.GetDataAsync(request.Type, request.ObjectId);
                return new RequestDataShardResponse()
                {
                    IsSuccessful = finalObject != null,
                    Data = finalObject,
                    NodeId = FoundOnNode,
                    ShardId = FoundShard,
                    SearchMessage = finalObject != null ? null : "Object " + request.ObjectId + " could not be found in shards.",
                    LockId = lockId,
                    AppliedLocked = appliedLock
                };
            }
        }
        #endregion

        #region Internal Parallel Calls
        public async void SendHeartbeats()
        {
            Logger.LogDebug(GetNodeId() + "Sending heartbeats");
            var startTime = DateTime.Now;

            var recognizedhosts = 1;

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
                        entriesToSend = _nodeStorage.Logs.GetRange(NextIndex[connector.Key] == 0 ? 0 : NextIndex[connector.Key] - 1, quantityToSend < _clusterOptions.MaxLogsToSend ? quantityToSend : _clusterOptions.MaxLogsToSend).ToList();
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

                    if (result.IsSuccessful)
                    {
                        Logger.LogDebug(GetNodeId() + "Successfully updated logs on " + connector.Key);
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
                            Logger.LogDebug(GetNodeId() + "Updated match index to " + MatchIndex);
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
                        Logger.LogError(GetNodeId() + "Append entry returned with undefined conflict name");
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

                    Interlocked.Increment(ref recognizedhosts);
                }
                catch (TaskCanceledException e)
                {
                    Logger.LogWarning(GetNodeId() + "Heartbeat to node " + connector.Key + " timed out");

                }
                catch (Exception e)
                {
                    Logger.LogWarning(GetNodeId() + "Encountered error while sending heartbeat to node " + connector.Key + ", request failed with error \"" + e.Message + " STACK TRACE:" + e.StackTrace);//+ "\"" + e.StackTrace);
                }
            });

            await Task.WhenAll(tasks);

            //If less then the required number recognize the request, go back to being a candidate
            if (recognizedhosts < _clusterOptions.MinimumNodes)
            {
                SetNodeRole(NodeState.Candidate);
            }
        }

        public async void StartElection()
        {
            try
            {
                if (MyUrl != null && CurrentState == NodeState.Candidate)
                {
                    Logger.LogDebug(GetNodeId() + "Starting election for term " + (_nodeStorage.CurrentTerm + 1) + ".");
                    var lastLogTerm = _nodeStorage.GetLastLogTerm();
                    // Caters for conditions when the node starts up and self elects to a greater term with two other nodes in Consensus
                    if (_nodeStorage.CurrentTerm > lastLogTerm + 3)
                    {
                        Logger.LogInformation("Detected that the node is too far ahead of its last log (3), restarting election from the term of the last log term " + lastLogTerm);
                        _nodeStorage.CurrentTerm = lastLogTerm;
                    }
                    else
                    {
                        SetCurrentTerm(_nodeStorage.CurrentTerm + 1);
                    }
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

                            if (result.IsSuccessful)
                            {
                                Interlocked.Increment(ref totalVotes);
                            }
                            else
                            {
                                Logger.LogDebug(GetNodeId() + " Node " + connector.Key + " rejected vote request.");
                            }
                        }
                        catch (TaskCanceledException e)
                        {
                            Logger.LogWarning(GetNodeId() + "Encountered error while getting vote from node " + connector.Key + ", request timed out...");
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
                        _nodeStorage.SetVotedFor(null);
                        SetNodeRole(NodeState.Follower);
                    }
                }
                else
                {
                    CurrentLeader = new KeyValuePair<Guid?, string>();
                    _nodeStorage.SetVotedFor(null);
                    SetNodeRole(NodeState.Follower);
                    Logger.LogWarning(GetNodeId() + "Cannot identify own URL to manage elections...");
                }
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to run election with error " + e.StackTrace);
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
                Logger.LogInformation(GetNodeId() + "Node's role changed to " + newState.ToString());
                CurrentState = newState;

                switch (newState)
                {
                    case NodeState.Candidate:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        //StopTimer(_clusterInfoTimeoutTimer);
                        InCluster = false;
                        StartElection();
                        break;
                    case NodeState.Follower:
                        //On becoming a follower, wait 5 seconds to allow any other nodes to send out election time outs
                        ResetTimer(_electionTimeoutTimer, rand.Next(_clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs * 2), _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        //(_clusterInfoTimeoutTimer);
                        //RestartTask(ref _clusterWatchTask, () => StartClusterWatchTask());
                        RestartTask(ref _taskWatchTask, () => GetTaskWatch());
                        RestartTask(ref _commitTask, () => MonitorCommits());
                        RestartTask(ref _nodeSelfHealingTask, NodeSelfHealingThread);
                        break;
                    case NodeState.Leader:
                        CompletedFirstLeaderDiscovery = false;
                        CurrentLeader = new KeyValuePair<Guid?, string>(_nodeStorage.Id, MyUrl);
                        ResetLeaderState();
                        ResetTimer(_heartbeatTimer, 0, _clusterOptions.ElectionTimeoutMs / 4);
                        //ResetTimer(_clusterInfoTimeoutTimer, 0, 1000);
                        RestartTask(ref _clusterWatchTask, () => StartClusterWatchTask());
                        StopTimer(_electionTimeoutTimer);
                        RestartTask(ref _commitTask, () => MonitorCommits());
                        RestartTask(ref _indexCreationTask, () => StartIndexCreation());
                        RestartTask(ref _taskWatchTask, () => GetTaskWatch());
                        InCluster = true;
                        RestartTask(ref _nodeSelfHealingTask, NodeSelfHealingThread);
                        RestartTask(ref _shardReassignmentTask, () => StartShardReassignment());
                        break;
                    case NodeState.Disabled:
                        StopTimer(_electionTimeoutTimer);
                        StopTimer(_heartbeatTimer);
                        // StopTimer(_clusterInfoTimeoutTimer);
                        break;
                }
            }
        }

        object locker = new object();

        private void RestartTask(ref Task task, Func<Task> threadFunction)
        {
            if (task == null || task.IsCompleted)
            {
                task = Task.Run(() => threadFunction());
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

                    List<SharedShardMetadata> Shards = new List<SharedShardMetadata>();

                    for (var i = 0; i < _clusterOptions.NumberOfShards; i++)
                    {
                        Shards.Add(new SharedShardMetadata()
                        {
                            InsyncAllocations = eligbleNodes.Keys.ToHashSet(),
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
        /// 
        /// </summary>
        /// <param name="operation"></param>
        /// <param name="shardId"></param>
        /// <param name="type"></param>
        /// <param name="pos"></param>
        /// <param name="data">correct data</param>
        public async void ReverseLocalTransaction(ShardOperation operation, Guid shardId, string type, int pos, ShardData correctData = null)
        {
            try
            {
                Logger.LogWarning(GetNodeId() + " Reverting operation " + ":" + pos + " " + operation.Operation.ToString() + " on shard " + shardId + " for object " + operation.ObjectId + Environment.NewLine + JsonConvert.SerializeObject(correctData));
                _nodeStorage.RemoveOperation(shardId, pos);
            }
            catch (ShardOperationConcurrencyException e)
            {
                Logger.LogError(GetNodeId() + " tried to remove a sync position out of order, " + e.Message + Environment.NewLine + e.StackTrace);
                throw e;
            }
            //Exception might be thrown because the sync position might be equal to the position you are setting
            catch (Exception e)
            {
                throw e;
            }

            switch (operation.Operation)
            {
                case ShardOperationOptions.Create:
                    var data = await _dataRouter.GetDataAsync(type, operation.ObjectId);
                    if (data != null)
                    {
                        Logger.LogInformation("Reverting create with deletion of " + data);
                        await _dataRouter.DeleteDataAsync(data);
                    }
                    break;
                // Put the data back in the right position
                case ShardOperationOptions.Delete:
                    // Set correct data for null for a full rollback
                    if (correctData != null)
                        await _dataRouter.InsertDataAsync(correctData);
                    break;
                case ShardOperationOptions.Update:
                    if (correctData != null)
                    {
                        data = await _dataRouter.GetDataAsync(type, operation.ObjectId);
                        if (data != null)
                            await _dataRouter.UpdateDataAsync(correctData);
                        else
                            await _dataRouter.InsertDataAsync(correctData);
                    }
                    else
                    {
                        Logger.LogError("Failed to revert update as there was no correct data given.");
                    }
                    break;
            }
        }

        /// <summary>
        /// Sync the local shard with the clusters shard. Used mainly if the node is out of sync with the cluster.
        /// </summary>
        /// <param name="shardId"></param>
        /// <returns></returns>
        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            Logger.LogDebug(GetNodeId() + "Starting sync of " + shardId + " on node " + MyUrl);
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
            SharedShardMetadata shardMetadata = null;// = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
            HttpNodeConnector selectedConnector = null;
            var startTime = DateTime.Now;
            while (selectedConnector == null)
            {
                shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
                //Time out finding a allocation
                if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                {
                    Logger.LogError("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                    throw new ClusterOperationTimeoutException("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                }

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

            //Start by checking whether the last 3 transaction was consistent
            Logger.LogDebug(GetNodeId() + " rechecking consistency for shard " + shard.ShardId);

            if (shard.SyncPos != 0)
            {
                var isConsistent = false;
                var positionsToCheck = 10;
                while (!isConsistent)
                {
                    var pointToCheckFrom = shard.SyncPos < 2 ? shard.SyncPos : shard.SyncPos - positionsToCheck;
                    int? lowestPosition = null;

                    var from = shard.SyncPos < positionsToCheck ? 0 : shard.SyncPos - positionsToCheck;
                    var to = shard.SyncPos;

                    Logger.LogWarning(GetNodeId() + " pulling operation " + shard.ShardId + " from:" + from + " to:" + to);
                    var lastOperation = await selectedConnector.Send(new RequestShardOperations()
                    {
                        From = from,
                        To = to,
                        ShardId = shardId,
                        Type = type,
                        IncludeOperations = true
                    });

                    if (!lastOperation.IsSuccessful)
                    {
                        Logger.LogError(GetNodeId() + " could not successfully fetch operations for shard " + shard.ShardId + " from:" + from + " to:" + to);
                        throw new Exception(GetNodeId() + " could not successfully fetch operations." + shard.ShardId + " from:" + from + " to:" + to);
                    }

                    // If you have more operations then the primary, delete the local operations
                    var currentOperationCount = _nodeStorage.GetCurrentShardLatestCount(shard.ShardId);
                    if (lastOperation.LatestPosition < currentOperationCount)
                    {
                        Logger.LogWarning(GetNodeId() + "Found local operations is ahead of primary, rolling back operations");
                        for (var i = currentOperationCount; i > lastOperation.LatestPosition; i--)
                        {
                            var operation = _nodeStorage.GetOperation(shardId, i);
                            ReverseLocalTransaction(operation, shard.ShardId, shard.Type, i, null);
                        }
                    }

                    foreach (var pos in lastOperation.Operations.OrderByDescending(o => o.Key))
                    {
                        Logger.LogWarning(GetNodeId() + "Checking operation " + pos.Key);
                        var myCopyOfTheTransaction = _nodeStorage.GetShardOperation(shard.ShardId, pos.Key);

                        if (myCopyOfTheTransaction == null || myCopyOfTheTransaction.ObjectId != pos.Value.ObjectId || myCopyOfTheTransaction.Operation != pos.Value.Operation)
                        {
                            Logger.LogWarning(GetNodeId() + "Found my copy of the operation" + pos.Key + " was not equal to the primary. Reverting operation " + Environment.NewLine + JsonConvert.SerializeObject(myCopyOfTheTransaction));
                            //Reverse the transaction
                            ReverseLocalTransaction(new ShardOperation()
                            {
                                Applied = false,
                                ObjectId = myCopyOfTheTransaction.ObjectId,
                                Operation = myCopyOfTheTransaction.Operation
                            }, shard.ShardId, shard.Type, pos.Key, pos.Value.Payload);
                            if (lowestPosition == null || lowestPosition > pos.Key)
                            {
                                lowestPosition = pos.Key;
                            }
                        }
                    }

                    Logger.LogError("My current operation count is " + currentOperationCount + " the remote operation count is " + lastOperation.LatestPosition);

                    if (lowestPosition == null)
                    {
                        isConsistent = true;
                        Logger.LogDebug(GetNodeId() + "Detected three consistently transactions, continuing recovery for shard " + shard.ShardId);
                    }
                    else
                    {
                        Logger.LogWarning(GetNodeId() + " reverted sync position to " + _nodeStorage.GetShardSyncPositions()[shard.ShardId]);
                        shard = _nodeStorage.GetShardMetadata(shardId);
                    }
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

                    var syncTo = lastPrimaryPosition > (shard.SyncPos + _clusterOptions.MaxObjectSync) ? (shard.SyncPos + _clusterOptions.MaxObjectSync) : lastPrimaryPosition;
                    Logger.LogDebug(GetNodeId() + "Syncing from " + (shard.SyncPos + 1) + " to " + syncTo);
                    var nextOperations = await selectedConnector.Send(new RequestShardOperations()
                    {
                        From = shard.SyncPos + 1,
                        // do 100 records at a time
                        To = syncTo,
                        ShardId = shardId,
                        Type = type
                    });

                    foreach (var operation in nextOperations.Operations)
                    {
                        Logger.LogDebug(GetNodeId() + "Recovering operation " + operation.Value.Position + " on shard " + shard.ShardId);
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

                        if (!result.IsSuccessful)
                        {
                            throw new Exception("Failed to replicate shard " + shard.ShardId + " operation " + operation.Value.Operation.ToString() + " for object " + operation.Value.ObjectId);
                        }
                        if (result.LatestPosition > lastPrimaryPosition)
                        {
                            Logger.LogDebug(GetNodeId() + "Updating the sync length to " + lastPrimaryPosition + " for shard " + shard.ShardId);
                            lastPrimaryPosition = result.LatestPosition;
                        }
                    }

                    currentPos = _nodeStorage.GetShardMetadata(shardId).SyncPos;

                    if (syncTo == lastPrimaryPosition)
                    {
                        Logger.LogDebug(GetNodeId() + "Recovery options finished current position " + currentPos + " latest position " + lastPrimaryPosition);

                        //If there is no next latest position, wait and check after a second
                        if (lastPrimaryPosition == nextOperations.LatestPosition)
                        {
                            Logger.LogDebug(GetNodeId() + "Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
                            shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);

                            //Update cluster to make this node in-sync
                            await Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>()
                            {
                                new UpdateShardMetadataAllocations(){
                                    ShardId = shardMetadata.Id,
                                    Type = shardMetadata.Type,
                                    InsyncAllocationsToAdd = new HashSet<Guid>(){_nodeStorage.Id },
                                    StaleAllocationsToRemove = new HashSet<Guid>(){_nodeStorage.Id }
                                }
                            },
                                WaitForCommits = true
                            });

                            while (!_stateMachine.GetShard(type, shardId).InsyncAllocations.Contains(_nodeStorage.Id))
                            {
                                Thread.Sleep(100);
                                Logger.LogDebug(GetNodeId() + "Awaiting the shard " + shardId + " to be insync.");
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
                                Logger.LogDebug(GetNodeId() + "Detected that shard " + shardMetadata.Id + " has had transient transactions. Continuing to resync.");
                            }
                            else
                            {
                                Logger.LogDebug(GetNodeId() + "Everything on " + shardMetadata.Id + " has been synced and the shard is now insync.");
                            }
                        }
                        else
                        {
                            Logger.LogDebug(GetNodeId() + "Detected shard " + shard.ShardId + " on " + MyUrl + " has a new latest sync position " + lastPrimaryPosition + " < " + nextOperations.LatestPosition);
                            lastPrimaryPosition = nextOperations.LatestPosition;
                        }
                    }
                }
            }
            //If the shard is insync but just not marked correctly, set the shard as insync
            else if (shardMetadata.StaleAllocations.Contains(_nodeStorage.Id))
            {
                //Update cluster to make this node in-sync
                await Send(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                            {
                                new UpdateShardMetadataAllocations(){
                                    ShardId = shardMetadata.Id,
                                    Type = shardMetadata.Type,
                                    //Remove this node from stale operations
                                    StaleAllocationsToRemove = new HashSet<Guid>(){  _nodeStorage.Id },
                                    InsyncAllocationsToAdd = new HashSet<Guid>(){  _nodeStorage.Id }
                                }
                            },
                    WaitForCommits = true
                });
            }
            Logger.LogDebug(GetNodeId() + "Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
            return true;
        }

        public void SetCurrentTerm(int newTerm)
        {
            _nodeStorage.SetCurrentTerm(newTerm);
        }

        public string GetNodeId()
        {
            return "Node:" + _nodeStorage.Id + "(" + MyUrl + "): ";
        }

        public bool HasEntryBeenCommitted(int logIndex)
        {
            if (CommitIndex >= logIndex)
            {
                return true;
            }
            return false;
        }

        public List<BaseTask> GetClusterTasks()
        {
            return _stateMachine.CurrentState.ClusterTasks.Select(ct => ct.Value).ToList();
        }

        public List<LogEntry> GetLogs()
        {
            return _nodeStorage.Logs;
        }

        public bool IsUptoDate()
        {
            return CommitIndex == _nodeStorage.GetLogCount();
        }
    }
}