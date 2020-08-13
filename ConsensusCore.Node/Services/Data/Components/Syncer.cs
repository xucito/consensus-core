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
            //ResyncShardWriteOperations();
        }

        /// <summary>
        /// Apply all unapplied operations
        /// </summary>
        /*public async void ResyncShardWriteOperations()
        {
            _logger.LogDebug("Resyncing Operations");

            foreach (var shard in await _shardRepository.GetAllShardMetadataAsync())
            {
                _logger.LogInformation("Checking shard " + shard.ShardId);
                var totalOperations = _shardRepository.GetTotalShardWriteOperationsCount(shard.ShardId);
                if (totalOperations > 0)
                {
                    var lastOperationAppliedPos = await _writer.GetLastOperationApplied(shard.ShardId);
                    var lastOperation = await _shardRepository.GetShardWriteOperationAsync(shard.ShardId, totalOperations);
                    if (lastOperation.Pos != lastOperationAppliedPos)
                    {
                        _writer.ReverseLocalTransaction(shard.ShardId, shard.Type, lastOperation.Pos);
                    }
                }

                _logger.LogDebug("Finished Resyncing Operations");
            }
        }*/



        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            var lastOperation = _writer.GetLastOperationCache(shardId);
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
                var totalPositions = _shardRepository.GetLastShardWriteOperationPos(shardId);
                //If the primary has less operations
                if (result.LatestPosition < lastOperationPos)
                {
                    while (totalPositions != result.LatestPosition)
                    {
                        await _writer.ReverseLocalTransaction(shardId, type, totalPositions);
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
                            await _writer.ReverseLocalTransaction(shardId, type, currentOperation.Pos);
                            currentPosition--;
                        }
                    }

                    if (_writer.GetLastOperationCache(shardId) == null && currentPosition != 0)
                    {
                        _logger.LogError("No cache available however current position is " + currentPosition);
                    }
                    else if (_writer.GetLastOperationCache(shardId) != null && _writer.GetLastOperationCache(shardId).Pos != currentPosition)
                    {
                        _logger.LogError("Failed to update last operation cache after reverting operations...");
                    }

                    result = await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                    {
                        ShardId = shardId,
                        From = currentPosition + 1,
                        To = currentPosition + 50
                    });

                    var totalShards = (_shardRepository.GetLastShardWriteOperationPos(shardId));
                    //If you have more operations
                    if (result.LatestPosition < totalShards)
                    {
                        _logger.LogWarning("Detected more nodes locally then primary, revering to latest position");
                        while (totalShards != result.LatestPosition)
                        {
                            await _writer.ReverseLocalTransaction(shardId, type, totalShards);
                            totalShards--;
                        }
                    }

                    while (result.LatestPosition > (_shardRepository.GetLastShardWriteOperationPos(shardId)))
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
