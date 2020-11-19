using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.Utility;
using ConsensusCore.Node.Communication.Exceptions;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Data.Components.ValueObjects;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data.Components
{
    public class Writer<State> where State : BaseState, new()
    {
        private readonly IDataRouter _dataRouter;
        private readonly IShardRepository _shardRepository;
        private readonly IStateMachine<State> _stateMachine;
        private readonly NodeStateService _nodeStateService;
        private readonly ClusterClient _clusterClient;
        private readonly ILogger _logger;
        private ConcurrentDictionary<Guid, Task> _replicationThreads = new ConcurrentDictionary<Guid, Task>();
        private ConcurrentDictionary<Guid, Channel<ShardWriteOperation>> _replicationQueues = new ConcurrentDictionary<Guid, Channel<ShardWriteOperation>>();
        private ConcurrentDictionary<Guid, int> _lastReplicatedPosition = new ConcurrentDictionary<Guid, int>();
        //Dictionary to check what the last pos of the operation applied was
        private ConcurrentDictionary<Guid, ShardWriteOperation> _lastOperationAppliedCache = new ConcurrentDictionary<Guid, ShardWriteOperation>();
        public object Info
        {
            get
            {
                return new
                {
                    LastReplicaticatedPositions = _lastReplicatedPosition,
                    LastOperationAppliedCache = _lastOperationAppliedCache
                };
            }
        }

        public Writer(
            ILogger<Writer<State>> logger,
            IShardRepository shardRepository,
            IDataRouter dataRouter,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient)
        {
            _logger = logger;
            _dataRouter = dataRouter;
            _shardRepository = shardRepository;
            _stateMachine = stateMachine;
            _nodeStateService = nodeStateService;
            _clusterClient = clusterClient;
            //populate the local operation cacheite
            PopulateLastOperationAppliedCache().GetAwaiter().GetResult();
        }


        public ShardWriteOperation GetLastOperationCache(Guid shardId)
        {
            _lastOperationAppliedCache.TryGetValue(shardId, out ShardWriteOperation operation);
            return operation;
        }

        private async Task<bool> PopulateLastOperationAppliedCache()
        {
            foreach (var shard in await _shardRepository.GetAllShardMetadataAsync())
            {
                var lastOperation = _shardRepository.GetLastShardWriteOperationPos(shard.ShardId);//await GetLastOperationApplied(shard.ShardId);
                if (lastOperation != null)
                    _lastOperationAppliedCache.TryAdd(shard.ShardId, await _shardRepository.GetShardWriteOperationAsync(shard.ShardId, lastOperation));
            }
            return true;
        }

        private async Task StartReplications(Guid nodeId)
        {
            while (await _replicationQueues[nodeId].Reader.WaitToReadAsync())
            {
                while (_replicationQueues[nodeId].Reader.TryRead(out ShardWriteOperation item))
                {
                    if (_stateMachine.GetShard(item.Data.ShardType, item.Data.ShardId.Value).InsyncAllocations.Contains(nodeId))
                    {
                        bool setNodeAsStale = false;
                        try
                        {
                            var replicationRequest = await _clusterClient.Send(nodeId, new ReplicateShardWriteOperation()
                            {
                                Metric = true,
                                Operation = item
                            });

                            setNodeAsStale = !replicationRequest.IsSuccessful;
                        }
                        catch (Exception e)
                        {
                            _logger.LogError("Failed to replicate transaction with error " + e.Message + Environment.NewLine + e.StackTrace);
                            setNodeAsStale = true;
                        }

                        if (setNodeAsStale)
                        {
                            _logger.LogWarning("Setting " + nodeId + " as stale.");

                            await _clusterClient.Send(new ExecuteCommands()
                            {
                                Commands = new List<BaseCommand>()
                                {
                                    new UpdateShardMetadataAllocations()
                                    {
                                        InsyncAllocationsToRemove = new HashSet<Guid>(){ nodeId },
                                        StaleAllocationsToAdd = new HashSet<Guid>(){ nodeId },
                                        ShardId = item.Data.ShardId.Value,
                                        Type =item.Data.ShardType
                                    }
                                },
                                WaitForCommits = true
                            });
                            //Move back the replication queue 1
                            _lastReplicatedPosition.TryUpdate(item.Data.ShardId.Value, item.Pos - 1, item.Pos);
                            return;
                        }
                    }
                }
            }
        }

        public async Task<WriteShardDataResponse> WriteShardData(ShardData data, ShardOperationOptions operationType, string operationId, DateTime transactionDate)
        {
            ShardWriteOperation operation = new ShardWriteOperation()
            {
                Data = data,
                Id = operationId,
                Operation = operationType,
                TransactionDate = transactionDate
            };

            ShardWriteOperation lastOperation;
            bool valueExists = _lastOperationAppliedCache.TryGetValue(data.ShardId.Value, out lastOperation);

            //Start at 1
            operation.Pos = !valueExists ? 1 : lastOperation.Pos + 1;
            var hash = lastOperation == null ? "" : lastOperation.ShardHash;
            operation.ShardHash = ObjectUtility.HashStrings(hash, operation.Id);
            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "writing new operation " + operationId + " with data " + Environment.NewLine + JsonConvert.SerializeObject(data, Formatting.Indented));

            var writeOperation = await _shardRepository.AddShardWriteOperationAsync(operation); //Add shard operation
            if (writeOperation)
            {
                try
                {
                    await ApplyOperationToDatastore(operation);
                    //Mark operation as applied
                    _lastOperationAppliedCache.AddOrUpdate(operation.Data.ShardId.Value, operation, (key, value) =>
                    {
                        if (value.Pos == operation.Pos - 1)
                        {
                            value = operation;
                        }
                        else
                        {
                            throw new WriteConcurrencyException("Failed to update the cache as the last operation applied was different from expected.");
                        }
                        return value;
                    });
                }
                catch (Exception e)
                {
                    _logger.LogError("Failed to apply " + operation.Operation.ToString() + " to shard " + operation.Data.ShardId + " for record " + operation.Data.Id);
                    return new WriteShardDataResponse()
                    {
                        IsSuccessful = false
                    };
                }

                //        Console.WriteLine(JsonConvert.SerializeObject(_stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value).InsyncAllocations, Formatting.Indented) + Environment.NewLine +  JsonConvert.SerializeObject(_stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value).StaleAllocations, Formatting.Indented));

                var tasks = _stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value).InsyncAllocations.Where(id => id != _nodeStateService.Id).Select(async allocation =>
                 {
                     _logger.LogInformation("Replicating operation from " + _nodeStateService.Id + " to " + allocation);
                     if (!_replicationQueues.ContainsKey(allocation))
                     {
                         if (_replicationQueues.TryAdd(allocation, Channel.CreateUnbounded<ShardWriteOperation>()))
                         {
                             _replicationThreads.TryAdd(allocation, Task.Run(async () =>
                             {
                                 await StartReplications(allocation);
                             }));

                             _lastReplicatedPosition.TryAdd(allocation, operation.Pos);
                         }
                     }
                     else if (_replicationThreads[allocation].IsCompleted)
                     {
                         _replicationThreads[allocation] = Task.Run(async () =>
                         {
                             await StartReplications(allocation);
                         });
                     }
                     var startTime = DateTime.Now;
                     while (_lastReplicatedPosition[allocation] < operation.Pos - 1)
                     {
                         if ((DateTime.Now - startTime).TotalMilliseconds > 60000)
                         {
                             throw new Exception("Replication time-out");
                         }
                         await Task.Delay(100);
                     }
                     await _replicationQueues[allocation].Writer.WriteAsync(operation);
                     _lastReplicatedPosition.TryUpdate(allocation, operation.Pos, operation.Pos - 1);
                 });

                await Task.WhenAll(tasks);

                //Console.WriteLine("Finished write " + operation.Pos);

                return new WriteShardDataResponse()
                {
                    Pos = operation.Pos,
                    ShardHash = operation.ShardHash,
                    IsSuccessful = true
                };
            }
            else
            {
                return new WriteShardDataResponse()
                {
                    IsSuccessful = false
                };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="operation"></param>
        /// <param name="forceReplicate">This is used to force replication of failed transactions</param>
        /// <returns>Last shard operation applied</returns>
        public async Task<ShardWriteOperation> ReplicateShardWriteOperationAsync(Guid shardId, ShardWriteOperation operation, bool forceReplicate)
        {
            ShardWriteOperation lastOperation = null;
            if (operation.Pos > 1)
            {
                if (!_lastOperationAppliedCache.TryGetValue(shardId, out lastOperation) || lastOperation == null)
                {
                    if (lastOperation == null)
                    {
                        return null;
                    }
                }

                if (lastOperation.Pos != operation.Pos - 1)
                {
                    throw new Exception("Tried to replicate out of order last operation is " + lastOperation.Pos + " and new operation is " + operation.Pos + ".");
                }

                string newHash;
                if ((newHash = ObjectUtility.HashStrings(lastOperation.ShardHash, operation.Id)) != operation.ShardHash)
                {
                    //Critical error with data concurrency, revert back to last known good commit via the resync process
                    _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to check the hash for operation " + operation.Id + ", marking shard as out of sync...");
                    throw new Exception(_nodeStateService.GetNodeLogId() + "Failed to check the hash for operation " + operation.Id + ", marking shard as out of sync...");
                }

                if (lastOperation.Pos >= operation.Pos)
                {
                    _logger.LogDebug("Found that operation " + operation.Pos + " for shard " + shardId + " has already occurred.");
                    return lastOperation;
                }
            }
            else
            {
                _lastOperationAppliedCache.TryAdd(shardId, null);
            }

            await _shardRepository.AddShardWriteOperationAsync(operation);
            try
            {
                await ApplyOperationToDatastore(operation);
                //Mark operation as applied
                if (!_lastOperationAppliedCache.TryUpdate(operation.Data.ShardId.Value, operation, lastOperation))
                {
                    if (_lastOperationAppliedCache[operation.Data.ShardId.Value].Id != operation.Id)
                        throw new WriteConcurrencyException("Failed to update the cache as the last operation (" + _lastOperationAppliedCache[operation.Data.ShardId.Value].Id + " applied was different from expected(" + operation.Id + ").");
                }
            }
            catch (Exception e)
            {
                _logger.LogError("Encountered error while trying to apply operation to datastore" + e.Message + Environment.NewLine + e.StackTrace);
                //Remove the operation
                await ReverseLocalTransaction(shardId, operation.Data.ShardType, operation.Pos);
                return _lastOperationAppliedCache[shardId];
            }
            return operation;
        }



        public async Task<bool> ReverseLocalTransaction(Guid shardId, string type, int pos)
        {
            _logger.LogWarning(_nodeStateService.GetNodeLogId() + "Reverting data on shard " + shardId);
            var operation = await _shardRepository.GetShardWriteOperationAsync(shardId, pos);
            if (operation != null)
            {
                await _shardRepository.AddDataReversionRecordAsync(new DataReversionRecord()
                {
                    OriginalOperation = operation,
                    NewData = operation.Data,
                    OriginalData = await _dataRouter.GetDataAsync(type, operation.Data.Id),
                    RevertedTime = DateTime.Now
                });

                ShardData data = operation.Data;
                ShardWriteOperation lastObjectOperation = null;
                if (operation.Operation == ShardOperationOptions.Update || operation.Operation == ShardOperationOptions.Delete)
                {
                    var allOperations = (await _shardRepository.GetAllObjectShardWriteOperationAsync(data.ShardId.Value, data.Id)).Select(ao => ao.Value);
                    lastObjectOperation = allOperations.LastOrDefault();
                }

                if (lastObjectOperation != null)
                {
                    switch (operation.Operation)
                    {
                        case ShardOperationOptions.Create:
                            data = await _dataRouter.GetDataAsync(type, operation.Data.Id);
                            if (data != null)
                            {
                                _logger.LogInformation(_nodeStateService.GetNodeLogId() + "Reverting create with deletion of " + data);
                                await _dataRouter.DeleteDataAsync(data);
                            }
                            break;
                        // Put the data back in the right position
                        case ShardOperationOptions.Delete:
                            await _dataRouter.InsertDataAsync(lastObjectOperation.Data);
                            break;
                        case ShardOperationOptions.Update:
                            if ((await _dataRouter.GetDataAsync(type, operation.Data.Id)) != null)
                                await _dataRouter.UpdateDataAsync(lastObjectOperation.Data);
                            else
                                await _dataRouter.InsertDataAsync(lastObjectOperation.Data);
                            break;
                    }
                }

                await _shardRepository.RemoveShardWriteOperationAsync(shardId, operation.Pos);
            }

            while (!_lastOperationAppliedCache.TryUpdate(shardId, await _shardRepository.GetShardWriteOperationAsync(shardId, pos - 1), _lastOperationAppliedCache[shardId]))
            {
                _logger.LogError("Trying to revert cache failed...");
            }

            return true;
        }

        private async Task<bool> ApplyOperationToDatastore(ShardWriteOperation operation)
        {
            try
            {
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
                _logger.LogDebug("Successfully applied operation " + Environment.NewLine + JsonConvert.SerializeObject(operation, Formatting.Indented));
            }
            catch (Exception e)
            {
                //_logger.LogError("Failed to apply operation to datastore with error " + Environment.NewLine + JsonConvert.SerializeObject(operation, Formatting.Indented) + e.Message + Environment.NewLine + e.StackTrace);
                throw e;
            }
            return true;
        }

        private async Task<int> GetLastOperationApplied(Guid shardId)
        {
            var lastPosition = 0; //if(_lastOperationAppliedCache.ContainsKey)//_shardRepository.GetLastShardWriteOperationPos(shardId);

            if (!_lastOperationAppliedCache.ContainsKey(shardId))
            {
                return 0;
            }
            else
            {
                lastPosition = _lastOperationAppliedCache[shardId].Pos;
            }

            var lastOperation = _lastOperationAppliedCache[shardId]; //await _shardRepository.GetShardWriteOperationAsync(shardId, lastPosition);

            if (lastOperation == null)
            {
                return lastPosition - 1;
            }

            bool isOperationApplied = true;
            ShardData data;
            switch (lastOperation.Operation)
            {
                //If entity exists
                case ShardOperationOptions.Create:
                    data = await _dataRouter.GetDataAsync(lastOperation.Data.ShardType, lastOperation.Data.Id);
                    if (data == null)
                        isOperationApplied = false;
                    break;
                case ShardOperationOptions.Delete:
                    data = await _dataRouter.GetDataAsync(lastOperation.Data.ShardType, lastOperation.Data.Id);
                    if (data != null)
                        isOperationApplied = false;
                    break;
                case ShardOperationOptions.Update:
                    data = await _dataRouter.GetDataAsync(lastOperation.Data.ShardType, lastOperation.Data.Id);
                    if (data.GetHashCode() != lastOperation.Data.GetHashCode())
                    {
                        isOperationApplied = false;
                    }
                    break;
            }
            if (isOperationApplied)
            {
                return lastPosition;
            }
            else
            {
                return lastPosition - 1;
            }
        }
    }
}
