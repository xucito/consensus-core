using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.Utility;
using ConsensusCore.Node.Connectors;
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

namespace ConsensusCore.Node.Services
{
    public class ShardManager<State, ShardRepository>
        where State : BaseState, new()
        where ShardRepository : IShardRepository
    {

        //public NodeStorage<State> _nodeStorage { get; set; }
        ILogger<ShardManager<State, ShardRepository>> Logger { get; set; }
        ClusterClient _clusterConnector;
        IStateMachine<State> _stateMachine;
        IDataRouter _dataRouter;
        ClusterOptions _clusterOptions;
        IShardRepository _shardRepository;
        Guid _nodeId;
        public string LogPrefix = "";

        //public ConcurrentDictionary<Guid, LocalShardMetaData> ShardMetaData { get; set; } = new ConcurrentDictionary<Guid, LocalShardMetaData>();
        /// <summary>
        /// A lock for every shard operation
        /// </summary>
        public ConcurrentDictionary<Guid, object> ShardWriteOperationLocks { get; set; } = new ConcurrentDictionary<Guid, object>();

        public ShardManager(IStateMachine<State> stateMachine,
            ILogger<ShardManager<State, ShardRepository>> logger,
            ClusterClient connector,
            IDataRouter dataRouter,
            IOptions<ClusterOptions> clusterOptions,
            // NodeStorage<State> nodeStorage,
            IShardRepository shardRepository,
            string logPrefix = "")
        {
            LogPrefix = logPrefix;
            _clusterConnector = connector;
            _stateMachine = stateMachine;
            _dataRouter = dataRouter;
            _clusterOptions = clusterOptions.Value;
            Logger = logger;
            _shardRepository = shardRepository;
        }

        public void SetNodeId(Guid id)
        {
            _nodeId = id;
        }

        public void GetOrAddLock(Guid objectId)
        {
            if (!ShardWriteOperationLocks.ContainsKey(objectId))
            {
                ShardWriteOperationLocks.TryAdd(objectId, new object());
            }
        }

        public IEnumerable<ShardWriteOperation> GetShardWriteOperations(Guid shardId)
        {
            return _shardRepository.GetAllShardWriteOperations(shardId);
        }
        

        public async Task<AddShardWriteOperationResponse> AddShardWriteOperation(ShardData data, ShardOperationOptions operationType, string operationId, DateTime transactionDate)
        {
            ShardWriteOperation operation = new ShardWriteOperation()
            {
                Data = data,
                Id = operationId,
                Operation = operationType,
                TransactionDate = transactionDate
            };
            //Start at 1
            operation.Pos = _shardRepository.GetTotalShardWriteOperationsCount(operation.Data.ShardId.Value) + 1;
            var hash = operation.Pos == 0 ? "" : _shardRepository.GetShardWriteOperation(operation.Data.ShardId.Value, operation.Pos - 1).ShardHash;
            operation.ShardHash = ObjectUtility.HashStrings(hash, operation.Id);
            //Write the data
            switch (operation.Operation)
            {
                case ShardOperationOptions.Create:
                    await _dataRouter.InsertDataAsync(operation.Data);
                    break;
                case ShardOperationOptions.Delete:
                    await _dataRouter.DeleteDataAsync(operation.Data);
                    break;
                case ShardOperationOptions.Update:
                    await _dataRouter.UpdateDataAsync(operation.Data);
                    break;
            }
            _shardRepository.AddShardWriteOperation(operation); //Add shard operation

            var shardMetadata = _stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value);
            ConcurrentBag<Guid> InvalidNodes = new ConcurrentBag<Guid>();
            //All allocations except for your own
            var tasks = shardMetadata.InsyncAllocations.Where(id => id != _nodeId).Select(async allocation =>
            {
                try
                {
                    var result = await _clusterConnector.Send(allocation, new ReplicateShardWriteOperation()
                    {
                        Operation = operation
                    });

                    if (result.IsSuccessful)
                    {
                        Logger.LogDebug(LogPrefix + "Successfully replicated all " + shardMetadata.Id + "shards.");
                    }
                    else
                    {
                        throw new Exception("Failed to replicate data to shard " + shardMetadata.Id + " to node " + allocation + " for operation " + operation.ToString() + Environment.NewLine + JsonConvert.SerializeObject(operation, Formatting.Indented));
                    }
                }
                catch (TaskCanceledException e)
                {
                    Logger.LogError(LogPrefix + "Failed to replicate shard " + shardMetadata.Id + " on shard " + _stateMachine.CurrentState.Nodes[allocation].TransportAddress + " for operation " + operation.Pos + " as request timed out, marking shard as not insync...");
                    InvalidNodes.Add(allocation);
                }
                catch (Exception e)
                {
                    Logger.LogError(LogPrefix + "Failed to replicate shard " + shardMetadata.Id + " for operation " + operation.Pos + ", marking shard as not insync..." + e.StackTrace);
                    InvalidNodes.Add(allocation);
                }
            });

            await Task.WhenAll(tasks);

            return new AddShardWriteOperationResponse()
            {
                FailedNodes = new List<Guid>(),
                IsSuccessful = true,
                Pos = operation.Pos,
                ShardId = operation.Data.ShardId.Value
            };
        }



        public async Task<ReplicateShardWriteOperationResponse> ReplicateShardWriteOperation(ShardWriteOperation operation)
        {
            // Get the last operation
            ShardWriteOperation lastOperation;

            var startTime = DateTime.Now;
            if (operation.Pos != 1)
            {
                while ((lastOperation = _shardRepository.GetShardWriteOperation(operation.Data.ShardId.Value, operation.Pos - 1)) == null)
                {
                    //Assume in the next 3 seconds you should receive the previos requests
                    if ((DateTime.Now - startTime).TotalMilliseconds > 3000)
                    {
                        return new ReplicateShardWriteOperationResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                    Thread.Sleep(100);
                }

                string newHash;
                if ((newHash = ObjectUtility.HashStrings(lastOperation.ShardHash, operation.Id)) != operation.ShardHash)
                {
                    //Critical error with data concurrency, revert back to last known good commit via the resync process
                    return new ReplicateShardWriteOperationResponse()
                    {
                        IsSuccessful = false
                    };
                }
            }

            //Run shard operation
            switch (operation.Operation)
            {
                case ShardOperationOptions.Create:
                    await _dataRouter.InsertDataAsync(operation.Data);
                    break;
                case ShardOperationOptions.Delete:
                    await _dataRouter.DeleteDataAsync(operation.Data);
                    break;
                case ShardOperationOptions.Update:
                    await _dataRouter.UpdateDataAsync(operation.Data);
                    break;
            }
            _shardRepository.AddShardWriteOperation(operation);
            return new ReplicateShardWriteOperationResponse()
            {
                IsSuccessful = true,
                LatestPosition = operation.Pos
            };
        }

        public async Task<bool> RunDataOperation(ShardOperationOptions operation, ShardData data)
        {
            try
            {
                switch (operation)
                {
                    // Note that a key assumption is data with the same GUID is the same piece of data.
                    case ShardOperationOptions.Create:
                        await _dataRouter.InsertDataAsync(data);
                        break;
                    case ShardOperationOptions.Update:
                        if (!_shardRepository.IsObjectMarkedForDeletion(data.ShardId.Value, data.Id))
                        {
                            try
                            {
                                await _dataRouter.UpdateDataAsync(data);
                                Logger.LogDebug("Updated data " + data.Id + " data is now " + Environment.NewLine + JsonConvert.SerializeObject(await _dataRouter.GetDataAsync(data.ShardType, data.Id), Formatting.Indented));
                            }
                            catch (Exception e)
                            {
                                Logger.LogWarning(LogPrefix + "Failed to update data with exception " + e.Message + " trying to add the data instead.");
                                await _dataRouter.InsertDataAsync(data);
                            }
                        }
                        else
                        {
                            Logger.LogError("OBJECT " + data.Id + " IS MARKED FOR DELETION");
                        }
                        break;
                    case ShardOperationOptions.Delete:
                        if (_shardRepository.MarkObjectForDeletion(new ObjectDeletionMarker()
                        {
                            GeneratedOn = DateTime.UtcNow,
                            ObjectId = data.Id,
                            ShardId = data.ShardId.Value
                        }))
                        {
                            await _dataRouter.DeleteDataAsync(data);
                        }
                        else
                        {
                            Logger.LogError(LogPrefix + "Ran into error while deleting " + data.Id);
                            return false;
                        }
                        break;
                }
                return true;
            }
            catch (Exception e)
            {
                Logger.LogError(LogPrefix + "Failed to run data operation against shard " + data.Id + " with exception " + e.Message + Environment.NewLine + e.StackTrace);
                return false;
            }
        }

        public async Task<RequestShardWriteOperationsResponse> RequestShardWriteOperations(Guid shardId, int from, int to, string type)
        {
            SortedDictionary<int, ShardWriteOperation> FinalList = new SortedDictionary<int, ShardWriteOperation>();
            //foreach value in from - to, pull the operation and then pull the object from object router
            for (var i = from; i <= to; i++)
            {
                var operation = _shardRepository.GetShardWriteOperation(shardId, i);
                if (operation != null)
                {
                    FinalList.Add(i, operation);
                }
            }

            return new RequestShardWriteOperationsResponse()
            {
                IsSuccessful = true,
                LatestPosition = _shardRepository.GetTotalShardWriteOperationsCount(shardId),
                Operations = FinalList
            };
        }

        public async Task<RequestDataShardResponse> RequestDataShard(Guid objectId, string type, int timeoutMs, Guid? shardId = null)
        {
            Guid? FoundShard = null;
            Guid? FoundOnNode = null;
            var currentTime = DateTime.Now;

            if (shardId == null)
            {
                var shards = _stateMachine.GetAllPrimaryShards(type);

                bool foundResult = false;
                ShardData finalObject = null;
                var totalRespondedShards = 0;

                var tasks = shards.Select(async shard =>
                {
                    if (shard.Value != _nodeId)
                    {
                        try
                        {
                            var result = await _clusterConnector.Send(shard.Value, new RequestDataShard()
                            {
                                ObjectId = objectId,
                                ShardId = shard.Key, //Set the shard
                                Type = type
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
                            Logger.LogError(LogPrefix + "Error thrown while getting " + e.Message);
                        }
                    }
                    else
                    {
                        finalObject = await _dataRouter.GetDataAsync(type, objectId);
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
                    if ((DateTime.Now - currentTime).TotalMilliseconds > timeoutMs)
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
                    Type = type,
                    ShardId = FoundShard,
                    IsSuccessful = true,
                    Data = finalObject,
                    SearchMessage = finalObject != null ? null : "Object " + objectId + " could not be found in shards.",

                };
            }
            else
            {
                var finalObject = await _dataRouter.GetDataAsync(type, objectId);
                return new RequestDataShardResponse()
                {
                    IsSuccessful = finalObject != null,
                    Data = finalObject,
                    NodeId = FoundOnNode,
                    ShardId = FoundShard,
                    SearchMessage = finalObject != null ? null : "Object " + objectId + " could not be found in shards.",
                };
            }
        }

        public bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId)
        {
            return _shardRepository.IsObjectMarkedForDeletion(shardId, objectId);
        }


        public bool MarkObjectForDeletion(Guid shardId, Guid objectId)
        {
            return _shardRepository.MarkObjectForDeletion(new ObjectDeletionMarker()
            {
                GeneratedOn = DateTime.UtcNow,
                ShardId = shardId
            });
        }


        public bool ObjectIsMarkedForDeletion(Guid shardId, Guid objectId)
        {
            return _shardRepository.IsObjectMarkedForDeletion(shardId, objectId);
        }

        public int GetTotalShardWriteOperationCount(Guid shardId)
        {
            return _shardRepository.GetTotalShardWriteOperationsCount(shardId);
        }

        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            var lastOperation = _shardRepository.GetShardWriteOperation(shardId, _shardRepository.GetTotalShardWriteOperationsCount(shardId) - 1);
            var shardMetadata = _stateMachine.GetShard(type, shardId);

            var result = await _clusterConnector.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
            {
                ShardId = shardId,
                From = lastOperation.Pos,
                To = lastOperation.Pos
            });

            if (result.IsSuccessful)
            {
                //Check whether the hash is equal, if not equal roll back each transaction
                var currentPosition = lastOperation.Pos;
                ShardWriteOperation currentOperation = null;
                if (!(result.Operations[lastOperation.Pos].ShardHash == lastOperation.ShardHash))
                {
                    //While the shard position does not match 
                    while (!((await _clusterConnector.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                    {
                        ShardId = shardId,
                        From = currentPosition,
                        To = currentPosition
                    })).Operations[currentPosition].ShardHash != (currentOperation = _shardRepository.GetShardWriteOperation(shardId, currentPosition)).ShardHash))
                    {
                        Logger.LogInformation("Reverting transaction " + currentOperation.Pos + " on shard " + shardId);
                        ReverseLocalTransaction(shardId, type, currentOperation);
                        currentPosition--;
                    }
                }

                while ((result = await _clusterConnector.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                {
                    ShardId = shardId,
                    From = currentPosition + 1,
                    To = lastOperation.Pos + 50
                })).LatestPosition != (_shardRepository.GetTotalShardWriteOperationsCount(shardId) - 1))
                {
                    foreach (var operation in result.Operations)
                    {
                        await ReplicateShardWriteOperation(operation.Value);
                    }
                }

                Logger.LogInformation("Successfully recovered data on shard " + shardId);
                return true;
            }
            else
            {
                throw new Exception("Failed to fetch shard from primary.");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="operation"></param>
        /// <param name="shardId"></param>
        /// <param name="type"></param>
        /// <param name="pos"></param>
        /// <param name="originalData">Only use in testing, shard manager should search this</param>
        public async void ReverseLocalTransaction(Guid shardId, string type, ShardWriteOperation operation)
        {
            var pos = _shardRepository.GetTotalShardWriteOperationsCount(shardId);
            if (operation != null)
            {
                try
                {
                    Logger.LogWarning(LogPrefix + " Reverting operation " + ":" + pos + " " + operation.Operation.ToString() + " on shard " + shardId + " for object " + operation.Data.Id);
                    _shardRepository.RemoveShardWriteOperation(shardId, pos);

                }
                catch (ShardWriteOperationConcurrencyException e)
                {
                    Logger.LogError(LogPrefix + "Tried to remove a sync position out of order, " + e.Message + Environment.NewLine + e.StackTrace);
                    throw e;
                }
                //Exception might be thrown because the sync position might be equal to the position you are setting
                catch (Exception e)
                {
                    throw e;
                }

                _shardRepository.AddDataReversionRecord(new DataReversionRecord()
                {
                    OriginalOperation = operation,
                    NewData = operation.Data,
                    OriginalData = await _dataRouter.GetDataAsync(type, operation.Data.Id),
                    RevertedTime = DateTime.Now
                });

                ShardData data = operation.Data;
                SortedDictionary<int, ShardWriteOperation> allOperations;
                ShardWriteOperation lastObjectOperation = null;
                if (operation.Operation == ShardOperationOptions.Update || operation.Operation == ShardOperationOptions.Delete)
                {
                    allOperations = _shardRepository.GetAllObjectShardWriteOperation(data.ShardId.Value, data.Id);
                    lastObjectOperation = allOperations.Where(ao => ao.Key < operation.Pos).Last().Value;
                }


                switch (operation.Operation)
                {
                    case ShardOperationOptions.Create:
                        data = await _dataRouter.GetDataAsync(type, operation.Data.Id);
                        if (data != null)
                        {
                            Logger.LogInformation(LogPrefix + "Reverting create with deletion of " + data);
                            await _dataRouter.DeleteDataAsync(data);
                        }
                        break;
                    // Put the data back in the right position
                    case ShardOperationOptions.Delete:
                        await _dataRouter.InsertDataAsync(lastObjectOperation.Data);
                        break;
                    case ShardOperationOptions.Update:
                        await _dataRouter.UpdateDataAsync(lastObjectOperation.Data);
                        break;
                }

                _shardRepository.RemoveShardWriteOperation(shardId, operation.Pos);
            }
        }
    }
}