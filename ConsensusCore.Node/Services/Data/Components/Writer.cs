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
        //Stores a copy of the last operation completed for each shard
        private ConcurrentDictionary<Guid, ShardWriteOperation> _shardLastOperationCache = new ConcurrentDictionary<Guid, ShardWriteOperation>();
        //Dictionary to check what the last pos of the operation applied was
        private ConcurrentDictionary<Guid, int> _lastOperationAppliedCache = new ConcurrentDictionary<Guid, int>();

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

            foreach (var shard in _shardRepository.GetAllShardMetadataAsync().GetAwaiter().GetResult())
            {
                ShardWriteOperation lastOperation = GetOrPopulateOperationCache(shard.ShardId).GetAwaiter().GetResult();

                if (!_lastOperationAppliedCache.ContainsKey(shard.ShardId))
                {
                    _lastOperationAppliedCache.TryAdd(shard.ShardId, GetLastOperationApplied(shard.ShardId).GetAwaiter().GetResult());
                }
                var lastAppliedPos = _lastOperationAppliedCache[shard.ShardId];
                // There is a unapplied transaction
                if (lastOperation.Pos - 1 == lastAppliedPos)
                {
                    ApplyShardWriteOperation(lastOperation).GetAwaiter().GetResult();
                }
            }
        }

        private async Task<ShardWriteOperation> GetOrPopulateOperationCache(Guid shardId)
        {
            if (!_shardLastOperationCache.ContainsKey(shardId))
            {
                var totalOperations = _shardRepository.GetTotalShardWriteOperationsCount(shardId);
                if (totalOperations != 0)
                {
                    var lastOperation = await _shardRepository.GetShardWriteOperationAsync(shardId, totalOperations);
                    _shardLastOperationCache.TryAdd(shardId, lastOperation);
                }
                else
                {
                    return null;
                }
            }
            return _shardLastOperationCache[shardId];
        }

        private void UpdateOperationCache(Guid shardId, ShardWriteOperation operation)
        {
            operation.Applied = true;
            _shardLastOperationCache.AddOrUpdate(shardId, operation, (key, val) => operation);
        }

        public async Task<bool> ApplyShardWriteOperation(ShardWriteOperation operation)
        {
            ApplyOperationToDatastore(operation);
            //Mark operation as applied
            if (!_lastOperationAppliedCache.TryUpdate(operation.Data.ShardId.Value, operation.Pos, operation.Pos - 1))
            {
                throw new WriteConcurrencyException("Failed to update the cache as the last operation applied was different from expected.");
            }
            //Update the cache
            UpdateOperationCache(operation.Data.ShardId.Value, operation);
            return true;
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

            ShardWriteOperation lastOperation = await GetOrPopulateOperationCache(operation.Data.ShardId.Value);

            //Start at 1
            operation.Pos = lastOperation == null ? 1 : lastOperation.Pos + 1;
            if (lastOperation == null)
            {
                _lastOperationAppliedCache.TryAdd(operation.Data.ShardId.Value, 0);
            }

            if (operation.Pos > 1 && _lastOperationAppliedCache[operation.Data.ShardId.Value] != lastOperation.Pos)
            {
                throw new WriteConcurrencyException("Encountered write exception as last write operation (" + lastOperation.Pos + ") was not equal to the last applied operation " + _lastOperationAppliedCache[operation.Data.ShardId.Value] + ".");
            }
            var hash = lastOperation == null ? "" : lastOperation.ShardHash;
            operation.ShardHash = ObjectUtility.HashStrings(hash, operation.Id);
            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "writing new operation " + operationId + " with data " + Environment.NewLine + JsonConvert.SerializeObject(data, Formatting.Indented));
            //Write the data

            var writeOperation = await _shardRepository.AddShardWriteOperationAsync(operation); //Add shard operation
            if (writeOperation)
            {
                try
                {
                    await ApplyShardWriteOperation(operation);
                }
                catch (Exception e)
                {
                    _logger.LogError("Failed to apply " + operation.Operation.ToString() + " to shard " + operation.Data.ShardId + " for record " + operation.Data.Id);
                    return new WriteShardDataResponse()
                    {
                        IsSuccessful = false
                    };
                }
                ConcurrentBag<Guid> InvalidNodes = new ConcurrentBag<Guid>();
                var shardMetadata = _stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value);
                //All allocations except for your own
                var tasks = shardMetadata.InsyncAllocations.Where(id => id != _nodeStateService.Id).Select(async allocation =>
                {
                    try
                    {
                        var result = await _clusterClient.Send(allocation, new ReplicateShardWriteOperation()
                        {
                            Operation = operation
                        });

                        if (result.IsSuccessful)
                        {
                            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Successfully replicated all " + shardMetadata.Id + "shards.");
                        }
                        else
                        {
                            throw new Exception("Failed to replicate data to shard " + shardMetadata.Id + " to node " + allocation + " for operation " + operation.ToString() + Environment.NewLine + JsonConvert.SerializeObject(operation, Formatting.Indented));
                        }
                    }
                    catch (TaskCanceledException e)
                    {
                        _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to replicate shard " + shardMetadata.Id + " on node " + allocation + " for operation " + operation.Pos + " as request timed out, marking shard as not insync...");
                        InvalidNodes.Add(allocation);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to replicate shard " + shardMetadata.Id + " for operation " + operation.Pos + " with error " + e.Message + ", marking shard as not insync..." + Environment.NewLine + e.StackTrace);
                        InvalidNodes.Add(allocation);
                    }
                });

                await Task.WhenAll(tasks);

                if (InvalidNodes.Count() > 0)
                {
                    await _clusterClient.Send(new ExecuteCommands()
                    {
                        Commands = new List<BaseCommand>()
                {
                    new UpdateShardMetadataAllocations()
                    {
                        ShardId = data.ShardId.Value,
                        Type = data.ShardType,
                        StaleAllocationsToAdd = InvalidNodes.ToHashSet(),
                        InsyncAllocationsToRemove = InvalidNodes.ToHashSet()
                    }
                },
                        WaitForCommits = true
                    });
                    _logger.LogInformation(_nodeStateService.GetNodeLogId() + " had stale virtual machines.");
                }

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
        /// <returns></returns>
        public async Task<bool> ReplicateShardWriteOperationAsync(Guid shardId, ShardWriteOperation operation, bool forceReplicate)
        {
            if (operation == null)
            {
                Console.WriteLine("Was asked to replicate a null transaction.");
                return true;
            }
            // Get the last operation
            ShardWriteOperation lastOperation;
            var startTime = DateTime.Now;

            var lastAppliedPos = 0;

            if (!_lastOperationAppliedCache.ContainsKey(operation.Data.ShardId.Value))
            {
                _lastOperationAppliedCache.TryAdd(operation.Data.ShardId.Value, await GetLastOperationApplied(operation.Data.ShardId.Value));
            }
            lastAppliedPos = _lastOperationAppliedCache[operation.Data.ShardId.Value];

            if (operation.Pos != 1)
            {
                if (lastAppliedPos != operation.Pos - 1)
                {
                    throw new Exception("Replicating transaction out of order.");
                }

                if (!forceReplicate)
                    while ((lastOperation = await GetOrPopulateOperationCache(shardId)) == null || !lastOperation.Applied || lastOperation.Pos < operation.Pos - 1)
                    {
                        //Assume in the next 3 seconds you should receive the previos requests
                        if ((DateTime.Now - startTime).TotalMilliseconds > 3000)
                        {
                            return false;
                        }
                        await Task.Delay(100);
                    }
                else
                    lastOperation = await GetOrPopulateOperationCache(shardId);

                if (lastOperation != null && lastOperation.Pos != operation.Pos - 1)
                {
                    lastOperation = await _shardRepository.GetShardWriteOperationAsync(shardId, operation.Pos - 1);
                }

                if (lastOperation != null)
                {
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
                        return true;
                    }
                }

                if (lastAppliedPos != lastOperation.Pos)
                {
                    throw new WriteConcurrencyException("Encountered write exception as last write operation (" + lastOperation.Pos + ") was not equal to the last applied operation " + _lastOperationAppliedCache[operation.Data.ShardId.Value] + ".");
                }
            }

            _shardRepository.AddShardWriteOperationAsync(operation).GetAwaiter().GetResult();
            try
            {
                ApplyOperationToDatastore(operation);
            }
            catch (Exception e)
            {
                Console.WriteLine("TRIPPED THE WIRE" + Environment.NewLine + e.StackTrace);
                return false;
            }
            //Mark operation as applied
            if (!_lastOperationAppliedCache.TryUpdate(operation.Data.ShardId.Value, operation.Pos, lastAppliedPos))
            {
                throw new WriteConcurrencyException("Failed to update the cache as the last operation (" + _lastOperationAppliedCache[operation.Data.ShardId.Value] + " applied was different from expected(" + lastAppliedPos + ").");
            }
            //Update the cache
            UpdateOperationCache(shardId, operation);
            return true;
        }



        public async void ReverseLocalTransaction(Guid shardId, string type, int pos)
        {
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
                SortedDictionary<int, ShardWriteOperation> allOperations;
                ShardWriteOperation lastObjectOperation = null;
                if (operation.Operation == ShardOperationOptions.Update || operation.Operation == ShardOperationOptions.Delete)
                {
                    allOperations = await _shardRepository.GetAllObjectShardWriteOperationAsync(data.ShardId.Value, data.Id);
                    lastObjectOperation = allOperations.Where(ao => ao.Key < operation.Pos).Last().Value;
                }

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

                await _shardRepository.RemoveShardWriteOperationAsync(shardId, operation.Pos);
            }

            //Update the cache
            _shardLastOperationCache.Remove(shardId, out _);
        }

        private async void ApplyOperationToDatastore(ShardWriteOperation operation)
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
                    try
                    {
                        await _dataRouter.UpdateDataAsync(operation.Data);
                    }
                    catch (Exception e)
                    {
                        if (operation.Operation == ShardOperationOptions.Update)
                        {
                            var allOperations = await _shardRepository.GetAllObjectShardWriteOperationAsync(operation.Data.ShardId.Value, operation.Data.Id);
                            if (allOperations.Where(ao => ao.Value.Operation == ShardOperationOptions.Delete).Count() == 0 && allOperations.Where(ao => ao.Value.Operation == ShardOperationOptions.Create).Count() == 1)
                            {
                                throw e;
                            }
                        }
                    }
                    break;
            }
        }

        public async Task<int> GetLastOperationApplied(Guid shardId)
        {
            var lastPosition = _shardRepository.GetTotalShardWriteOperationsCount(shardId);

            if (lastPosition == 0)
            {
                return 0;
            }
            var lastOperation = await _shardRepository.GetShardWriteOperationAsync(shardId, lastPosition);

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
