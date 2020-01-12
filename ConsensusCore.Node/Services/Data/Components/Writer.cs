﻿using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.Utility;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

        public async Task<bool> WriteShardData(ShardData data, ShardOperationOptions operationType, string operationId, DateTime transactionDate)
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
            var hash = operation.Pos == 1 ? "" : (await _shardRepository.GetShardWriteOperationAsync(operation.Data.ShardId.Value, operation.Pos - 1)).ShardHash;
            operation.ShardHash = ObjectUtility.HashStrings(hash, operation.Id);
            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "writing new operation " + operationId + " with data " + Environment.NewLine + JsonConvert.SerializeObject(data, Formatting.Indented));
            //Write the data

            var writeOperation = await _shardRepository.AddShardWriteOperationAsync(operation); //Add shard operation
            if (writeOperation)
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
                var shardMetadata = _stateMachine.GetShard(operation.Data.ShardType, operation.Data.ShardId.Value);
                //Mark operation as applied
                await _shardRepository.MarkShardWriteOperationAppliedAsync(operation.Id);
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

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}