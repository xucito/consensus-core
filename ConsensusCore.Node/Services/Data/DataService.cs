using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.SystemCommands.Tasks;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Data.Components;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.SystemTasks;
using ConsensusCore.Node.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data
{
    public class DataService<State> : IDataService where State : BaseState, new()
    {
        private Task _writeTask;
        private Task _indexCreationTask;
        private Task _allocationTask;
        private Task _replicaValidationChecksTask;

        private readonly IShardRepository _shardRepository;
        private readonly ILogger _logger;
        private readonly WriteCache _writeCache;
        private readonly ClusterClient _clusterClient;
        private readonly IStateMachine<State> _stateMachine;
        private readonly ClusterOptions _clusterOptions;
        ConcurrentQueue<string> IndexCreationQueue { get; set; } = new ConcurrentQueue<string>();
        private readonly NodeStateService _nodeStateService;

        //Internal Components
        private readonly Writer<State> _writer;
        private readonly Allocator<State> _allocator;
        private readonly Syncer<State> _syncer;
        private readonly Reader<State> _reader;

        public DataService(
            ILoggerFactory loggerFactory,
            IShardRepository shardRepository,
            WriteCache writeCache,
            IDataRouter dataRouter,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient,
            IOptions<ClusterOptions> clusterOptions)
        {
            _nodeStateService = nodeStateService;
            _clusterOptions = clusterOptions.Value;
            _stateMachine = stateMachine;
            _writeCache = writeCache;
            _logger = loggerFactory.CreateLogger<DataService<State>>();
            _shardRepository = shardRepository;
            _clusterClient = clusterClient;
            _reader = new Reader<State>(
                loggerFactory.CreateLogger<Reader<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient); ;
            _allocator = new Allocator<State>(
                loggerFactory.CreateLogger<Allocator<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient);
            _writer = new Writer<State>(loggerFactory.CreateLogger<Writer<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient
                );
            _syncer = new Syncer<State>(shardRepository, loggerFactory.CreateLogger<Syncer<State>>(), dataRouter, stateMachine, clusterClient, nodeStateService);
            _writeTask = new Task(async () =>
            {
                while (true)
                {
                    var operation = _writeCache.DequeueOperation();
                    if (operation != null)
                        await _writer.WriteShardData(operation.Data, operation.Operation, operation.Id, operation.TransactionDate);
                    else
                    {
                        //Release write thread for a small amount of time
                        Thread.Sleep(100);
                    }
                }
            });
            _writeTask.Start();
            TaskUtility.RestartTask(ref _indexCreationTask, () => CreateIndexLoop());

            _allocationTask = new Task(async () => await AllocateShards());
            _allocationTask.Start();
        }

        public async Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {
            try
            {
                DateTime commandStartTime = DateTime.Now;
                TResponse response;
                switch (request)
                {
                    case RequestDataShard t1:
                        response = (TResponse)(object)await RequestDataShardHandler(t1);
                        break;
                    case AddShardWriteOperation t1:
                        response = (TResponse)(object)await AddShardWriteOperationHandler(t1);
                        break;
                    case RequestCreateIndex t1:
                        response = (TResponse)(object)CreateIndexHandler(t1);
                        break;
                    case AllocateShard t1:
                        response = (TResponse)(object)await AllocateShardHandler(t1);
                        break;
                    case ReplicateShardWriteOperation t1:
                        response = (TResponse)(object)await ReplicateShardWriteOperationHandler(t1);
                        break;
                    case RequestShardWriteOperations t1:
                        response = (TResponse)(object)await RequestShardWriteOperationsHandler(t1);
                        break;
                    case RequestShardSync t1:
                        response = (TResponse)(object)await RequestShardSyncHandler(t1);
                        break;
                    default:
                        throw new Exception("Request is not implemented");
                }

                return response;
            }
            catch (TaskCanceledException e)
            {
                _logger.LogWarning(_nodeStateService.GetNodeLogId() + "Request " + request.RequestName + " timed out...");
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
            catch (Exception e)
            {
                _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to handle request " + request.RequestName + " with error " + e.Message + Environment.StackTrace + e.StackTrace);
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
        }

        public async Task<RequestShardSyncResponse> RequestShardSyncHandler(RequestShardSync request)
        {
            return new RequestShardSyncResponse()
            {
                IsSuccessful = await _syncer.SyncShard(request.ShardId, request.Type)
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

            if (request.CreateLock)
            {
                try
                {
                    if (!_stateMachine.IsObjectLocked(request.ObjectId))
                    {
                        Guid newLockId = Guid.NewGuid();
                        await Handle(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>{
                                    new SetObjectLock()
                                    {
                                        ObjectId = request.ObjectId,
                                        Type = request.Type,
                                        LockId = newLockId,
                                        TimeoutMs = request.LockTimeoutMs
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

            data = new RequestDataShardResponse()
            {
                Data = await _reader.GetData(request.ObjectId, request.Type, request.TimeoutMs),
                IsSuccessful = true
            };
            data.LockId = lockId;
            data.AppliedLocked = appliedLock;
            return data;
        }

        public async Task<AddShardWriteOperationResponse> AddShardWriteOperationHandler(AddShardWriteOperation request)
        {
            var startDate = DateTime.Now;
            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Received write request for object " + request.Data.Id + " for request " + request.Data.ShardId);
            AddShardWriteOperationResponse finalResult = new AddShardWriteOperationResponse();
            //Check if index exists, if not - create one
            if (!_stateMachine.IndexExists(request.Data.ShardType))
            {
                await _clusterClient.Send(_nodeStateService.CurrentLeader.Value, new RequestCreateIndex()
                {
                    Type = request.Data.ShardType
                });

                DateTime startIndexCreation = DateTime.Now;
                while (!_stateMachine.IndexExists(request.Data.ShardType))
                {
                    if ((DateTime.Now - startIndexCreation).Milliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        throw new IndexCreationFailedException("Index creation for shard " + request.Data.ShardType + " timed out.");
                    }
                    Thread.Sleep(100);
                }
            }

            ShardAllocationMetadata shardMetadata;

            if (request.Data.ShardId == null)
            {
                var allocations = _stateMachine.GetShards(request.Data.ShardType);
                Random rand = new Random();
                var selectedNodeIndex = rand.Next(0, allocations.Length);
                request.Data.ShardId = allocations[selectedNodeIndex].Id;
                shardMetadata = allocations[selectedNodeIndex];
            }
            else
            {
                shardMetadata = _stateMachine.GetShard(request.Data.ShardType, request.Data.ShardId.Value);
            }

            //If the shard is assigned to you
            if (shardMetadata.PrimaryAllocation == _nodeStateService.Id)
            {
                var operationId = Guid.NewGuid().ToString();
                _writeCache.EnqueueOperation(new ShardWriteOperation()
                {
                    Data = request.Data,
                    Id = operationId,
                    Operation = request.Operation,
                    TransactionDate = DateTime.Now
                });
                finalResult.IsSuccessful = true;

                while (!_writeCache.IsOperationComplete(operationId))
                {
                    if ((DateTime.Now - startDate).Milliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        throw new IndexCreationFailedException("Queue clearance for transaction " + operationId + request.Data.ShardType + " timed out.");
                        throw new IndexCreationFailedException("Queue clearance for transaction " + operationId + request.Data.ShardType + " timed out.");
                    }
                    Thread.Sleep(1);
                }
            }
            else
            {
                try
                {
                    return await _clusterClient.Send(shardMetadata.PrimaryAllocation, request);
                }
                catch (Exception e)
                {
                    _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to write " + request.Operation.ToString() + " request to primary node " + _stateMachine.CurrentState.Nodes[shardMetadata.PrimaryAllocation].TransportAddress + " for object " + request.Data.Id + " shard " + request.Data.ShardId + "|" + e.StackTrace);
                    throw e;
                }
            }

            if (request.RemoveLock)
            {
                var result = await Handle(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                        {
                            new RemoveObjectLock()
                            {
                                ObjectId = request.Data.Id,
                                Type = shardMetadata.Type
                            }
                },
                    WaitForCommits = true
                });

                if (result.IsSuccessful)
                {
                    finalResult.LockRemoved = true;
                }
            }

            return finalResult;
        }

        public async Task<RequestCreateIndexResponse> RequestCreateIndexHandler(RequestCreateIndex request)
        {
            IndexCreationQueue.Enqueue(request.Type);
            return new RequestCreateIndexResponse()
            {
                IsSuccessful = true
            };
        }

        public async Task<AllocateShardResponse> AllocateShardHandler(AllocateShard shard)
        {
            _allocator.AllocateShard(shard.ShardId, shard.Type);
            return new AllocateShardResponse() { };
        }

        private Task CreateIndexLoop()
        {
            while (true)
            {
                //TODO FIX THIS LOGIC
                if (_nodeStateService.Role == NodeState.Leader)
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
                                _logger.LogInformation(_nodeStateService.GetNodeLogId() + "Creating index for type " + typeToCreate);
                                var result = _allocator.CreateIndexAsync(typeToCreate, _clusterOptions.DataTransferTimeoutMs, _clusterOptions.NumberOfShards).GetAwaiter().GetResult();

                                DateTime startTime = DateTime.Now;
                                while (!_stateMachine.IndexExists(typeToCreate))
                                {
                                    if ((DateTime.Now - startTime).TotalMilliseconds < _clusterOptions.DataTransferTimeoutMs)
                                    {
                                        throw new Exception("Failed to create index " + typeToCreate + ", timed out index detection.");
                                    }
                                    _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Awaiting index creation.");
                                    Thread.Sleep(100);
                                }
                            }
                            else
                            {
                                _logger.LogDebug(_nodeStateService.GetNodeLogId() + "INDEX for type " + typeToCreate + " Already exists, skipping creation...");
                            }
                        }
                    }
                    while (isSuccessful);
                    Thread.Sleep(1000);
                }
                else
                {
                    Thread.Sleep(1000);
                }
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

        public async Task<RequestShardWriteOperationsResponse> RequestShardWriteOperationsHandler(RequestShardWriteOperations request)
        {
            return new RequestShardWriteOperationsResponse()
            {
                IsSuccessful = true,
                Operations = await _shardRepository.GetShardWriteOperationsAsync(request.ShardId, request.From, request.To),
                LatestPosition = _shardRepository.GetTotalShardWriteOperationsCount(request.ShardId)
            };
        }

        public async Task<ReplicateShardWriteOperationResponse> ReplicateShardWriteOperationHandler(ReplicateShardWriteOperation request)
        {
            return new ReplicateShardWriteOperationResponse()
            {
                IsSuccessful = await _syncer.ReplicateShardWriteOperationAsync(request.Operation)
            };
        }

        public async Task CheckAllReplicas()
        {
            //Get all nodes you are the primary for
            foreach (var shard in _stateMachine.GetAllPrimaryShards(_nodeStateService.Id))
            {
                //Get the shard positions
                var shardPosition = _shardRepository.GetTotalShardWriteOperationsCount(shard.Id);
                //Wait 2 times the latency tolerance
                Thread.Sleep(_clusterOptions.LatencyToleranceMs * 2);

                ConcurrentBag<Guid> staleNodes = new ConcurrentBag<Guid>();

                var tasks = shard.InsyncAllocations.Select(ia => new Task(async () =>
                {
                    var shardOperation = await _clusterClient.Send(new RequestShardWriteOperations()
                    {
                        From = 0,
                        To = 0,
                        ShardId = shard.Id,
                        Type = shard.Type
                    });

                    if (shardOperation.LatestPosition < shardPosition)
                    {
                        staleNodes.Add(ia);
                    }
                }));

                await Task.WhenAll(tasks);

                if (staleNodes.Count > 0)
                {
                    _logger.LogInformation(_nodeStateService.GetNodeLogId() + " primary detected stale nodes");
                    await _clusterClient.Send(new ExecuteCommands()
                    {
                        Commands = new List<BaseCommand>()
                        {
                            new UpdateShardMetadataAllocations()
                            {
                                StaleAllocationsToAdd = staleNodes.ToHashSet<Guid>(),
                                ShardId = shard.Id,
                                Type = shard.Type
                            }
                        }
                    });
                }
            }
        }

        public async Task AllocateShards()
        {
            while (true)
            {
                if (_nodeStateService.Role == NodeState.Leader)
                {
                    _logger.LogInformation("Allocating shards...");
                    var updates = new List<BaseCommand>();
                    var newTasks = new List<BaseTask>();
                    var allShards = _stateMachine.GetShards();

                    foreach (var shard in allShards)
                    {
                        //Scan for new allocations first
                        var newAllocations = _allocator.GetAllocationCandidates(shard.Id, shard.Type);
                        foreach (var newAllocation in newAllocations)
                        {
                            _logger.LogInformation("Found new allocation " + newAllocation.NodeId + " for shard " + shard.Id);
                            updates.Add(new UpdateShardMetadataAllocations()
                            {
                                StaleAllocationsToAdd = newAllocations.Select(na => na.NodeId).ToHashSet<Guid>(),
                                ShardId = shard.Id,
                                Type = shard.Type
                            });

                            foreach (var candidate in newAllocations)
                            {
                                var taskId = ResyncShard.GetTaskUniqueId(shard.Id, candidate.NodeId);
                                BaseTask recoveryTask = _stateMachine.GetRunningTask(taskId);
                                if (recoveryTask == null)
                                {
                                    newTasks.Add(new ResyncShard()
                                    {
                                        Id = Guid.NewGuid(),
                                        ShardId = shard.Id,
                                        NodeId = candidate.NodeId,
                                        Type = shard.Type,
                                        UniqueRunningId = taskId,
                                        CreatedOn = DateTime.UtcNow
                                    });
                                }
                            }
                        }

                        var staleAllocationsToRemove = new List<Guid>();
                        //Scan for stale Allocations
                        foreach (var staleAllocation in shard.StaleAllocations)
                        {
                            //If the node is just stale then try resync it
                            if (_stateMachine.GetNode(staleAllocation) != null)
                            {
                                _logger.LogInformation("Found stale allocation " + staleAllocation + " for shard " + shard.Id);
                                var taskId = ResyncShard.GetTaskUniqueId(shard.Id, staleAllocation);
                                BaseTask recoveryTask = _stateMachine.GetRunningTask(taskId);
                                if (recoveryTask == null)
                                {
                                    newTasks.Add(new ResyncShard()
                                    {
                                        Id = Guid.NewGuid(),
                                        ShardId = shard.Id,
                                        NodeId = staleAllocation,
                                        Type = shard.Type,
                                        UniqueRunningId = taskId,
                                        CreatedOn = DateTime.UtcNow
                                    });
                                }
                            }
                            else
                            {
                                staleAllocationsToRemove.Add(staleAllocation);
                            }
                        }

                        if (staleAllocationsToRemove.Count() > 0)
                        {
                            updates.Add(new UpdateShardMetadataAllocations()
                            {
                                ShardId = shard.Id,
                                Type = shard.Type,
                                StaleAllocationsToRemove = staleAllocationsToRemove.ToHashSet()
                            });
                        }

                        var stillInsync = shard.InsyncAllocations.Where(insync => _stateMachine.IsNodeContactable(insync));
                        var staleAllocations = shard.InsyncAllocations.Where(ia => !stillInsync.Contains(ia));
                        if (stillInsync.Count() > 0)
                        {
                            updates.Add(new UpdateShardMetadataAllocations()
                            {
                                ShardId = shard.Id,
                                Type = shard.Type,
                                PrimaryAllocation = stillInsync.Contains(shard.PrimaryAllocation) ? shard.PrimaryAllocation : stillInsync.First(),
                                StaleAllocationsToAdd = staleAllocations.ToHashSet(),
                                InsyncAllocationsToRemove = staleAllocations.ToHashSet()
                            });

                            //Scan for primary allocations or in-sync allocations becoming unavailable
                            foreach (var staleAllocation in staleAllocations)
                            {
                                _logger.LogInformation("Found stale allocation " + staleAllocation + " for shard " + shard.Id);
                                var taskId = ResyncShard.GetTaskUniqueId(shard.Id, staleAllocation);
                                BaseTask recoveryTask = _stateMachine.GetRunningTask(taskId);
                                if (recoveryTask == null)
                                {
                                    newTasks.Add(new ResyncShard()
                                    {
                                        Id = Guid.NewGuid(),
                                        ShardId = shard.Id,
                                        NodeId = staleAllocation,
                                        Type = shard.Type,
                                        UniqueRunningId = taskId,
                                        CreatedOn = DateTime.UtcNow
                                    });
                                }
                            }
                        }
                        else
                        {
                            _logger.LogError("Shard " + shard.Id + " has no primaries available, shard is unavailable...");
                        }

                        //Get the latest position of the shard
                        var latestOperation = await _clusterClient.Send(new RequestShardWriteOperations()
                        {
                            From = 0,
                            To = 0,
                            ShardId = shard.Id,
                            Type = shard.Type
                        });

                        if (_stateMachine.GetShard(shard.Type, shard.Id).LatestOperationPos != latestOperation.LatestPosition)
                        {
                            updates.Add(new UpdateShardMetadataAllocations()
                            {
                                ShardId = shard.Id,
                                Type = shard.Type,
                                LatestPos = latestOperation.LatestPosition
                            });
                        }
                    }

                    updates.Add(new UpdateClusterTasks()
                    {
                        TasksToAdd = newTasks
                    });


                    await _clusterClient.Send(new ExecuteCommands()
                    {
                        Commands = updates,
                        WaitForCommits = true
                    });

                    Thread.Sleep(3000);
                }
                else
                {
                    Thread.Sleep(5000);
                }
            }
        }
    }
}
