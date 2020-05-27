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
using Newtonsoft.Json;
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
        private Task _objectLockWatcher;
        private Task _staleDataCollectorTask;

        //Performance Objects
        ConcurrentDictionary<int, double> totals = new ConcurrentDictionary<int, double>();
        object totalLock = new object();
        int totalRequests = 0;

        private readonly IShardRepository _shardRepository;
        private readonly ILogger _logger;
        private readonly WriteCache _writeCache;
        private readonly ClusterClient _clusterClient;
        private readonly IStateMachine<State> _stateMachine;
        private readonly ClusterOptions _clusterOptions;
        ConcurrentQueue<string> IndexCreationQueue { get; set; } = new ConcurrentQueue<string>();
        private readonly NodeStateService _nodeStateService;
        private readonly NodeOptions _nodeOptions;
        private readonly StaleDataCollector _staleDataCollector;

        //Internal Components
        public readonly Writer<State> Writer;
        public readonly Allocator<State> Allocator;
        public readonly Syncer<State> Syncer;
        public readonly Reader<State> Reader;

        public DataService(
            ILoggerFactory loggerFactory,
            IShardRepository shardRepository,
            IDataRouter dataRouter,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient,
            IOptions<ClusterOptions> clusterOptions,
            IOperationCacheRepository transactionCacheRepository,
            IOptions<NodeOptions> nodeOptions)
        {
            _nodeStateService = nodeStateService;
            _nodeOptions = nodeOptions.Value;
            _clusterOptions = clusterOptions.Value;
            _stateMachine = stateMachine;
            _writeCache = new WriteCache(transactionCacheRepository, loggerFactory.CreateLogger<WriteCache>(), _nodeOptions.PersistWriteQueue);
            _logger = loggerFactory.CreateLogger<DataService<State>>();
            _shardRepository = shardRepository;
            _clusterClient = clusterClient;
            Reader = new Reader<State>(
                loggerFactory.CreateLogger<Reader<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient); ;
            Allocator = new Allocator<State>(
                loggerFactory.CreateLogger<Allocator<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient);
            Writer = new Writer<State>(loggerFactory.CreateLogger<Writer<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient
                );
            Syncer = new Syncer<State>(shardRepository, loggerFactory.CreateLogger<Syncer<State>>(), stateMachine, clusterClient, nodeStateService, Writer);
            _staleDataCollector = new StaleDataCollector(loggerFactory.CreateLogger<StaleDataCollector>(), shardRepository, dataRouter);


            _writeTask = new Task(async () =>
            {
                //Before you write you should first dequeue all transactions
                if (_writeCache.TransitQueue.Count() > 0)
                {
                    _logger.LogInformation("Found transactions in transit, attempting to reapply them...");
                    foreach (var operationKV in _writeCache.TransitQueue.ToDictionary(entry => entry.Key, entry => entry.Value))
                    {
                        var operation = operationKV.Value;
                        try
                        {
                            var result = await Writer.WriteShardData(operation.Data, operation.Operation, operation.Id, operation.TransactionDate);
                        }
                        catch (Exception e)
                        {
                            _logger.LogError("Failed to apply operation " + operation.Id + " with exception " + e.Message + Environment.NewLine + e.StackTrace);
                            try
                            {
                                await _writeCache.CompleteOperation(operation.Id);
                            }
                            catch (Exception completionError)
                            {
                                _logger.LogError("Error removing operation from transit queue with error " + completionError.Message + Environment.NewLine
                                    + completionError.StackTrace + Environment.NewLine + JsonConvert.SerializeObject(operation, Formatting.Indented));
                            }
                        }
                    }
                }

                while (true)
                {
                    try
                    {
                        var operation = await _writeCache.DequeueOperation();
                        if (operation != null)
                        {
                            try
                            {
                                var result = await Writer.WriteShardData(operation.Data, operation.Operation, operation.Id, operation.TransactionDate);
                            }
                            catch (Exception e)
                            {
                                _logger.LogError("Failed to write operation with exception " + e.Message + Environment.NewLine + JsonConvert.SerializeObject(operation, Formatting.Indented));
                            }
                            await _writeCache.CompleteOperation(operation.Id);
                        }
                        else
                        {
                            //Release write thread for a small amount of time
                            await Task.Delay(100);
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.LogError("Encountered critical write error " + e.Message + Environment.NewLine + e.StackTrace);
                    }
                }
            });
            _writeTask.Start();
            TaskUtility.RestartTask(ref _indexCreationTask, async () => await CreateIndexLoop());

            _allocationTask = new Task(async () => await AllocateShards());
            _allocationTask.Start();
            _replicaValidationChecksTask = Task.Run(async () => await CheckAllReplicas());

            _objectLockWatcher = new Task(async () => await CheckLocks());
            _objectLockWatcher.Start();

            _staleDataCollectorTask = Task.Run(async () =>
            {
                while (true)
                {
                    if (_nodeStateService.InCluster && !_nodeStateService.IsStale)
                    {
                        foreach (var shardMetadata in _stateMachine.GetShards().Where(s => s.StaleAllocations.Count() == 0 && s.InsyncAllocations.Contains(_nodeStateService.Id)))
                        {
                            var totalDeleted = await _staleDataCollector.CleanUpShard(shardMetadata.Id, _shardRepository.GetTotalShardWriteOperationsCount(shardMetadata.Id) - nodeOptions.Value.StaleDataTrailingLogCount);
                            if (totalDeleted > 0)
                                _logger.LogDebug("Removed " + totalDeleted + " operations from " + shardMetadata.Id);
                        }
                        await Task.Delay(_nodeOptions.StaleDataCleanupIntervalMs);
                    }
                    else
                    {
                        await Task.Delay(10000);
                    }
                }
            });


            if (_nodeOptions.EnablePerformanceLogging)
            {
                var performancePrinting = new Task(() =>
                {
                    while (true)
                    {
                        Console.Clear();
                        Console.WriteLine("Performance Report...");
                        foreach (var value in totals)
                        {
                            Console.WriteLine(value.Key + ":" + (value.Value / totalRequests));
                        }
                        Console.WriteLine("Queue:" + _writeCache.OperationsInQueue);
                        Task.Delay(1000);
                    }
                });
                performancePrinting.Start();
            }
        }

        public async Task CheckLocks()
        {
            while (true)
            {
                if (_nodeStateService.Role == NodeState.Leader)
                {
                    try
                    {
                        var objectLocks = _stateMachine.GetLocks();
                        List<Lock> expiredLocks = new List<Lock>();
                        foreach (var clusterLock in objectLocks)
                        {
                            if (clusterLock.Value.IsExpired())
                            {
                                _logger.LogDebug("Detected that lock for object " + clusterLock.Key + " is expired.");
                                expiredLocks.Add(clusterLock.Value);

                                //Clear the locks if you reach 50
                                if (expiredLocks.Count() > 50)
                                {
                                    await _clusterClient.Send(new ExecuteCommands()
                                    {
                                        Commands = expiredLocks.Select(el =>
                                               new RemoveLock()
                                               {
                                                   Name = el.Name,
                                                   LockId = el.LockId
                                               }).ToList(),
                                        WaitForCommits = true
                                    });
                                    expiredLocks = new List<Lock>();
                                }
                            }
                        }

                        if (expiredLocks.Count() > 0)
                        {
                            await _clusterClient.Send(new ExecuteCommands()
                            {
                                Commands = expiredLocks.Select(el =>
                                       new RemoveLock()
                                       {
                                           Name = el.Name,
                                           LockId = el.LockId
                                       }).ToList(),
                                WaitForCommits = true
                            });
                        }
                        await Task.Delay(100);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError("Failed to release locks with exception " + e.Message + Environment.NewLine + e.StackTrace);
                    }
                }
                else
                {
                    //Sleep for 10 seconds if you are not the leader.
                    await Task.Delay(10000);
                }
            }
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
                        response = (TResponse)(object)await RequestCreateIndexHandler(t1);
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
                IsSuccessful = await Syncer.SyncShard(request.ShardId, request.Type)
            };
        }

        private int requestThreads = 0;

        public async Task<RequestDataShardResponse> RequestDataShardHandler(RequestDataShard request)
        {
            if (requestThreads > 1000)
            {
                throw new Exception("Reached maximum concurrent search threads " + requestThreads + ".");
            }
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
                    if (!_stateMachine.IsLocked(request.GetLockName()))
                    {
                        Guid newLockId = Guid.NewGuid();
                        await _clusterClient.Send(new ExecuteCommands()
                        {
                            Commands = new List<BaseCommand>{
                                    new SetLock()
                                    {
                                        Name = request.GetLockName(),
                                        LockId = newLockId,
                                        TimeoutMs = request.LockTimeoutMs,
                                        CreatedOn = DateTime.Now
                                    }
                                },
                            WaitForCommits = true
                        });

                        var lockStartTime = DateTime.Now;
                        //While the object is not locked yet, wait
                        while (!_stateMachine.IsLocked(request.GetLockName()))
                        {
                            if ((DateTime.Now - lockStartTime).TotalMilliseconds > _clusterOptions.LatencyToleranceMs)
                            {
                                throw new ClusterOperationTimeoutException("Locking operation timed out on " + request.ObjectId);
                            }
                            await Task.Delay(100);
                        }

                        //After the objects been locked check the ID
                        if (!_stateMachine.IsLockObtained(request.GetLockName(), newLockId))
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
                        IsSuccessful = false,
                        AppliedLocked = false,
                        SearchMessage = "Object " + request.ObjectId + " is locked."
                    };
                }

            }

            try
            {
                Interlocked.Increment(ref requestThreads);
                data = new RequestDataShardResponse()
                {
                    Data = await Reader.GetData(request.ObjectId, request.Type, request.TimeoutMs),
                    IsSuccessful = true
                };
                Interlocked.Decrement(ref requestThreads);
            }
            catch (Exception e)
            {
                Console.WriteLine("Concurrent search threads running: " + requestThreads);
                Interlocked.Decrement(ref requestThreads);
                throw e;
            }
            data.LockId = lockId;
            data.AppliedLocked = appliedLock;
            return data;
        }


        public SemaphoreSlim writeCheckThreads = new SemaphoreSlim(5);

        public async Task<AddShardWriteOperationResponse> AddShardWriteOperationHandler(AddShardWriteOperation request)
        {
            var startDate = DateTime.Now;
            var checkpoint = 1;
            //var totalOperation = _writeCache.OperationQueue.Count();
            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Received write request for object " + request.Data.Id + " for request " + request.Data.ShardId);
            AddShardWriteOperationResponse finalResult = new AddShardWriteOperationResponse();
            //Check if index exists, if not - create one
            if (!_stateMachine.IndexExists(request.Data.ShardType))
            {
                if (IndexCreationQueue.Where(icq => icq == request.Data.ShardType).Count() > 0)
                {
                    while (!_stateMachine.IndexExists(request.Data.ShardType))
                    {
                        if ((DateTime.Now - startDate).Milliseconds > _clusterOptions.DataTransferTimeoutMs)
                        {
                            throw new IndexCreationFailedException("Index creation for shard " + request.Data.ShardType + " is already queued.");
                        }
                        await Task.Delay(10);
                    }
                }
                else
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
                        await Task.Delay(100);
                    }
                }
            }

            if (_nodeOptions.EnablePerformanceLogging) PerformanceMetricUtility.PrintCheckPoint(ref totalLock, ref totals, ref startDate, ref checkpoint, "index allocation");

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

            if (_nodeOptions.EnablePerformanceLogging) PerformanceMetricUtility.PrintCheckPoint(ref totalLock, ref totals, ref startDate, ref checkpoint, "shard Allocation");

            //If the shard is assigned to you
            if (shardMetadata.PrimaryAllocation == _nodeStateService.Id)
            {
                var operationId = Guid.NewGuid().ToString();
                finalResult.OperationId = operationId;
                await _writeCache.EnqueueOperationAsync(new ShardWriteOperation()
                {
                    Data = request.Data,
                    Id = operationId,
                    Operation = request.Operation,
                    TransactionDate = DateTime.Now
                });
                finalResult.IsSuccessful = true;

                if (_nodeOptions.EnablePerformanceLogging) PerformanceMetricUtility.PrintCheckPoint(ref totalLock, ref totals, ref startDate, ref checkpoint, "enqueue");

                ShardWriteOperation transaction;

                writeCheckThreads.Wait();
                while (!_writeCache.IsOperationComplete(operationId))
                {
                    if ((DateTime.Now - startDate).Milliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        throw new IndexCreationFailedException("Queue clearance for transaction " + operationId + request.Data.ShardType + " timed out.");
                    }
                    await Task.Delay(1);
                }
                writeCheckThreads.Release();
                // printCheckPoint(ref startDate, ref checkpoint, "wait for completion");
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
                var result = await _clusterClient.Send(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                        {
                            new RemoveLock()
                            {
                                Name = request.Data.GetLockName(),
                                LockId = request.LockId
                            }
                },
                    WaitForCommits = true
                });

                if (result.IsSuccessful)
                {
                    finalResult.LockRemoved = true;
                }
            }

            Interlocked.Increment(ref totalRequests);

            if (_nodeOptions.EnablePerformanceLogging) PerformanceMetricUtility.PrintCheckPoint(ref totalLock, ref totals, ref startDate, ref checkpoint, "removeLock");

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
            Allocator.AllocateShard(shard.ShardId, shard.Type);
            return new AllocateShardResponse() { };
        }

        private async Task CreateIndexLoop()
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
                                var result = Allocator.CreateIndexAsync(typeToCreate, _clusterOptions.DataTransferTimeoutMs, _clusterOptions.NumberOfShards).GetAwaiter().GetResult();

                                DateTime startTime = DateTime.Now;
                                while (!_stateMachine.IndexExists(typeToCreate))
                                {
                                    if ((DateTime.Now - startTime).TotalMilliseconds < _clusterOptions.DataTransferTimeoutMs)
                                    {
                                        throw new Exception("Failed to create index " + typeToCreate + ", timed out index detection.");
                                    }
                                    _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Awaiting index creation.");
                                    await Task.Delay(100);
                                }
                            }
                            else
                            {
                                _logger.LogDebug(_nodeStateService.GetNodeLogId() + "INDEX for type " + typeToCreate + " Already exists, skipping creation...");
                            }
                        }
                    }
                    while (isSuccessful);
                    await Task.Delay(1000);
                }
                else
                {
                    await Task.Delay(1000);
                }
            }
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
                IsSuccessful = await Writer.ReplicateShardWriteOperationAsync(request.Operation.Data.ShardId.Value, request.Operation, false)
            };
        }

        public async Task CheckAllReplicas()
        {
            while (true)
            {
                try
                {
                    _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Checking all replicas");
                    //Get all nodes you are the primary for
                    foreach (var shard in _stateMachine.GetAllPrimaryShards(_nodeStateService.Id))
                    {
                        //Get the shard positions
                        var shardPosition = _shardRepository.GetTotalShardWriteOperationsCount(shard.Id);
                        //Wait 2 times the latency tolerance
                        await Task.Delay(_clusterOptions.LatencyToleranceMs * 5);

                        ConcurrentBag<Guid> staleNodes = new ConcurrentBag<Guid>();

                        var tasks = shard.InsyncAllocations.Where(ia => ia != _nodeStateService.Id).Select(async ia =>
                        {
                            var shardOperation = await _clusterClient.Send(ia, new RequestShardWriteOperations()
                            {
                                From = 0,
                                To = 0,
                                ShardId = shard.Id,
                                Type = shard.Type
                            });

                            //If the operations are lagging or it is infront of the latest count (Old transactions)
                            if (shardOperation.LatestPosition < shardPosition || shardOperation.LatestPosition > shardPosition)
                            {
                                staleNodes.Add(ia);
                            }
                        });

                        await Task.WhenAll(tasks);

                        if (staleNodes.Count > 0)
                        {
                            _logger.LogDebug(_nodeStateService.GetNodeLogId() + " primary detected stale nodes");
                            await _clusterClient.Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>()
                        {
                            new UpdateShardMetadataAllocations()
                            {
                                InsyncAllocationsToRemove = staleNodes.ToHashSet<Guid>(),
                                StaleAllocationsToAdd = staleNodes.ToHashSet<Guid>(),
                                ShardId = shard.Id,
                                Type = shard.Type
                            }
                        },
                                WaitForCommits = true
                            });
                        }
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to check all replicas with exception " + e.Message + Environment.NewLine + e.StackTrace);
                }
                await Task.Delay(1000);
            }
        }

        public async Task AllocateShards()
        {
            while (true)
            {
                try
                {
                    if (_nodeStateService.Role == NodeState.Leader && _nodeStateService.InCluster)
                    {
                        _logger.LogDebug("Allocating shards...");
                        var updates = new List<BaseCommand>();
                        var newTasks = new List<BaseTask>();
                        var allShards = _stateMachine.GetShards();

                        foreach (var shard in allShards)
                        {
                            //Scan for new allocations first
                            var newAllocations = Allocator.GetAllocationCandidates(shard.Id, shard.Type);
                            if (newAllocations.Count() > 0)
                            {
                                _logger.LogInformation("Found new allocations for shard " + shard.Id);
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

                            //If there are new stale allocations
                            var stillInsync = shard.InsyncAllocations.Where(insync => _stateMachine.IsNodeContactable(insync));
                            var staleAllocations = shard.InsyncAllocations.Where(ia => !stillInsync.Contains(ia));
                            if (staleAllocations.Count() > 0)
                            {
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
                            }

                            //Get the latest position of the shard
                            var latestOperation = await _clusterClient.Send(new RequestShardWriteOperations()
                            {
                                From = 0,
                                To = 0,
                                ShardId = shard.Id,
                                Type = shard.Type
                            });
                        }

                        if (newTasks.Count > 0)
                        {
                            updates.Add(new UpdateClusterTasks()
                            {
                                TasksToAdd = newTasks
                            });
                        }

                        if (updates.Count > 0)
                        {
                            await _clusterClient.Send(new ExecuteCommands()
                            {
                                Commands = updates,
                                WaitForCommits = true
                            });
                        }

                        await Task.Delay(3000);
                    }
                    else
                    {
                        await Task.Delay(5000);
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError("Failed to allocate shards with error " + e.Message + Environment.NewLine + e.StackTrace);
                }
            }
        }
    }
}
