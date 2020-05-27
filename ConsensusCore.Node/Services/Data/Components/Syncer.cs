using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.Utility;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data.Components
{
    public class Syncer<State> where State : BaseState, new()
    {
        IShardRepository _shardRepository;
        ILogger _logger;
        IStateMachine<State> _stateMachine;
        ClusterClient _clusterClient;
        NodeStateService _nodeStateService;
        Writer<State> _writer;

        public Syncer(IShardRepository shardRepository,
            ILogger<Syncer<State>> logger,
            IStateMachine<State> stateMachine,
            ClusterClient clusterClient,
             NodeStateService nodeStateService,
             Writer<State> writer)
        {
            _logger = logger;
            _shardRepository = shardRepository;
            _stateMachine = stateMachine;
            _nodeStateService = nodeStateService;
            _clusterClient = clusterClient;
            _writer = writer;
            ResyncShardWriteOperations();
        }

        /// <summary>
        /// Apply all unapplied operations
        /// </summary>
        public async void ResyncShardWriteOperations()
        {
            _logger.LogDebug("Resyncing Operations");

            foreach (var shard in await _shardRepository.GetAllShardMetadataAsync())
            {
                _logger.LogInformation("Checking shard " + shard.ShardId);
                var unappliedShardOperations = await _shardRepository.GetAllUnappliedOperationsAsync(shard.ShardId);
                foreach (var unappliedShardOperation in unappliedShardOperations)
                {
                    ShardWriteOperation lastOperation;
                    //If the previous operation exists and has been applied
                    if ((lastOperation = await _shardRepository.GetShardWriteOperationAsync(unappliedShardOperation.Value.Data.ShardId.Value, unappliedShardOperation.Value.Pos - 1)) != null && lastOperation.Applied)
                    {
                        string newHash;

                        //Run shard operation
                        _writer.ApplyOperationToDatastore(unappliedShardOperation.Value);
                        await _shardRepository.MarkShardWriteOperationAppliedAsync(unappliedShardOperation.Value.Id);
                    }
                    //Reverse all the last operations
                    else
                    {
                        var removeFrom = unappliedShardOperation.Value.Pos;
                        for (var i = _shardRepository.GetTotalShardWriteOperationsCount(shard.ShardId); i >= removeFrom; i--)
                        {
                            _logger.LogWarning("Detected out of order operations from " + removeFrom + ", removing operations onwards.");
                            _writer.ReverseLocalTransaction(unappliedShardOperation.Value.Data.ShardId.Value, unappliedShardOperation.Value.Data.ShardType, i);
                        }
                        //Break out of loop
                        break;
                    }
                }
            }

            _logger.LogDebug("Finished Resyncing Operations");
        }



        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            var totalOperations = _shardRepository.GetTotalShardWriteOperationsCount(shardId);
            var lastOperation = totalOperations != 0 ? await _shardRepository.GetShardWriteOperationAsync(shardId, totalOperations) : null;
            int lastOperationPos = lastOperation == null ? 0 : lastOperation.Pos;

            var shardMetadata = _stateMachine.GetShard(type, shardId);

            var result = await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
            {
                ShardId = shardId,
                From = lastOperationPos,
                To = lastOperationPos + 1
            });

            if (result.IsSuccessful)
            {
                var totalPositions = _shardRepository.GetTotalShardWriteOperationsCount(shardId);
                //If the primary has less operations
                if (result.LatestPosition < lastOperationPos)
                {
                    while (totalPositions != result.LatestPosition)
                    {
                        _writer.ReverseLocalTransaction(shardId, type, totalPositions);
                        totalPositions--;
                    }
                }
                else
                {
                    //Check whether the hash is equal, if not equal roll back each transaction
                    var currentPosition = lastOperationPos;
                    ShardWriteOperation currentOperation = null;
                    if (lastOperationPos != 0 && result.Operations[lastOperationPos] != null && !(result.Operations[lastOperationPos].ShardHash == lastOperation.ShardHash))
                    {
                        //While the shard position does not match 
                        while (!((await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                        {
                            ShardId = shardId,
                            From = currentPosition,
                            To = currentPosition
                        })).Operations[currentPosition].ShardHash != (currentOperation = await _shardRepository.GetShardWriteOperationAsync(shardId, currentPosition)).ShardHash))
                        {
                            _logger.LogInformation("Reverting transaction " + currentOperation.Pos + " on shard " + shardId);
                            _writer.ReverseLocalTransaction(shardId, type, currentOperation.Pos);
                            currentPosition--;
                        }
                    }

                    result = await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                    {
                        ShardId = shardId,
                        From = currentPosition + 1,
                        To = currentPosition + 50
                    });

                    var totalShards = (_shardRepository.GetTotalShardWriteOperationsCount(shardId));
                    //If you have more operations
                    if (result.LatestPosition < totalShards)
                    {
                        _logger.LogWarning("Detected more nodes locally then primary, revering to latest position");
                        while (totalShards != result.LatestPosition)
                        {
                            _writer.ReverseLocalTransaction(shardId, type, totalShards);
                            totalShards--;
                        }
                    }

                    while (result.LatestPosition > (_shardRepository.GetTotalShardWriteOperationsCount(shardId)))
                    {
                        foreach (var operation in result.Operations)
                        {
                            _logger.LogDebug(_nodeStateService.Id + "Replicated operation " + operation.Key + " for shard " + shardId);
                            await _writer.ReplicateShardWriteOperationAsync(shardId, operation.Value, true);
                        }
                        //If it does equal zero, it means the transaction has been deleted
                        if (result.Operations.Count() > 0)
                            currentPosition = result.Operations.Last().Key;
                        else
                            currentPosition++;
                        result = await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                        {
                            ShardId = shardId,
                            From = currentPosition + 1,
                            To = currentPosition + 50
                        });
                    }
                }

                _logger.LogInformation("Successfully recovered data on shard " + shardId);
                return true;
            }
            else
            {
                throw new Exception("Failed to fetch shard from primary.");
            }
        }
    }
}
