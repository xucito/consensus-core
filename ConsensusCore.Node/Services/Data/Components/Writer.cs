﻿using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.Utility;
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
            var hash = lastOperation == null ? "" : lastOperation.ShardHash;
            operation.ShardHash = ObjectUtility.HashStrings(hash, operation.Id);
            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "writing new operation " + operationId + " with data " + Environment.NewLine + JsonConvert.SerializeObject(data, Formatting.Indented));
            //Write the data

            var writeOperation = await _shardRepository.AddShardWriteOperationAsync(operation); //Add shard operation
            if (writeOperation)
            {
                ApplyOperationToDatastore(operation);
                var shardMetadata = _stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value);
                //Mark operation as applied
                await _shardRepository.MarkShardWriteOperationAppliedAsync(operation.Id);
                //Update the cache
                UpdateOperationCache(operation.Data.ShardId.Value, operation);
                ConcurrentBag<Guid> InvalidNodes = new ConcurrentBag<Guid>();
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


        public async Task<bool> ReplicateShardWriteOperationAsync(ShardWriteOperation operation)
        {
            // Get the last operation
            ShardWriteOperation lastOperation;
            var startTime = DateTime.Now;
            if (operation.Pos != 1)
            {
                while ((lastOperation = await GetOrPopulateOperationCache(operation.Data.ShardId.Value)) == null || !lastOperation.Applied || lastOperation.Pos < operation.Pos - 1)
                {
                    //Assume in the next 3 seconds you should receive the previos requests
                    if ((DateTime.Now - startTime).TotalMilliseconds > 3000)
                    {
                        return false;
                    }
                    await Task.Delay(100);
                }

                if (lastOperation.Pos != operation.Pos - 1)
                {
                    lastOperation = await _shardRepository.GetShardWriteOperationAsync(operation.Data.ShardId.Value, operation.Pos - 1);
                }

                string newHash;
                if ((newHash = ObjectUtility.HashStrings(lastOperation.ShardHash, operation.Id)) != operation.ShardHash)
                {
                    //Critical error with data concurrency, revert back to last known good commit via the resync process
                    _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to check the hash for operation " + operation.Id + ", marking shard as out of sync...");
                    throw new Exception(_nodeStateService.GetNodeLogId() + "Failed to check the hash for operation " + operation.Id + ", marking shard as out of sync...");
                }
            }

            // if (operation.Pos > 1)
            //{
            //   lock (_shardLastOperationCache[operation.Data.ShardId.Value])
            //  {
            //The operation must already be being applied
            /* if (_shardLastOperationCache[operation.Data.ShardId.Value].Pos >= operation.Pos)
             {
                 throw new Exception("There must be a replication process being run");
             }*/

            _shardRepository.AddShardWriteOperationAsync(operation).GetAwaiter().GetResult();
            //Run shard operation
            ApplyOperationToDatastore(operation);
            _shardRepository.MarkShardWriteOperationAppliedAsync(operation.Id).GetAwaiter().GetResult();
            //Update the cache
            UpdateOperationCache(operation.Data.ShardId.Value, operation);
            //}
            /* }
             else
             {
                 await _shardRepository.AddShardWriteOperationAsync(operation);
                 //Run shard operation
                 ApplyOperationToDatastore(operation);
                 await _shardRepository.MarkShardWriteOperationAppliedAsync(operation.Id);
                 //Update the cache
                 UpdateOperationCache(operation.Data.ShardId.Value, operation);
             }*/

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
                        await _dataRouter.UpdateDataAsync(lastObjectOperation.Data);
                        break;
                }

                await _shardRepository.RemoveShardWriteOperationAsync(shardId, operation.Pos);
            }
            //Update the cache
            _shardLastOperationCache.Remove(operation.Data.ShardId.Value, out _);
        }

        public async void ApplyOperationToDatastore(ShardWriteOperation operation)
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
        }
    }
}
