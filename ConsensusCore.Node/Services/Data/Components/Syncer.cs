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
        IDataRouter _dataRouter;
        IStateMachine<State> _stateMachine;
        ClusterClient _clusterClient;
        NodeStateService _nodeStateService;

        public Syncer(IShardRepository shardRepository,
            ILogger<Syncer<State>> logger,
            IDataRouter dataRouter,
            IStateMachine<State> stateMachine,
            ClusterClient clusterClient,
             NodeStateService nodeStateService)
        {
            _logger = logger;
            _shardRepository = shardRepository;
            _dataRouter = dataRouter;
            _stateMachine = stateMachine;
            _nodeStateService = nodeStateService;
            _clusterClient = clusterClient;
        }

        public async Task<bool> ReplicateShardWriteOperationAsync(ShardWriteOperation operation)
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
                        return false;
                    }
                    Thread.Sleep(100);
                }

                string newHash;
                if ((newHash = ObjectUtility.HashStrings(lastOperation.ShardHash, operation.Id)) != operation.ShardHash)
                {
                    //Critical error with data concurrency, revert back to last known good commit via the resync process
                    return false;
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
            return true;
        }

        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            var lastOperation = _shardRepository.GetShardWriteOperation(shardId, _shardRepository.GetTotalShardWriteOperationsCount(shardId) - 1);
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
                //Check whether the hash is equal, if not equal roll back each transaction
                var currentPosition = lastOperationPos;
                ShardWriteOperation currentOperation = null;
                if (lastOperationPos != 0 && !(result.Operations[lastOperationPos].ShardHash == lastOperation.ShardHash))
                {
                    //While the shard position does not match 
                    while (!((await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                    {
                        ShardId = shardId,
                        From = currentPosition,
                        To = currentPosition
                    })).Operations[currentPosition].ShardHash != (currentOperation = _shardRepository.GetShardWriteOperation(shardId, currentPosition)).ShardHash))
                    {
                        _logger.LogInformation("Reverting transaction " + currentOperation.Pos + " on shard " + shardId);
                        ReverseLocalTransaction(shardId, type, currentOperation);
                        currentPosition--;
                    }
                }

                result = await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                {
                    ShardId = shardId,
                    From = currentPosition + 1,
                    To = currentPosition + 50
                });

                while (result.LatestPosition != (_shardRepository.GetTotalShardWriteOperationsCount(shardId)))
                {
                    foreach (var operation in result.Operations)
                    {
                        _logger.LogInformation(_nodeStateService.Id + "Replicated operation " + operation.Key + " for shard " + shardId);
                        await ReplicateShardWriteOperationAsync(operation.Value);
                    }
                    result = await _clusterClient.Send(shardMetadata.PrimaryAllocation, new RequestShardWriteOperations()
                    {
                        ShardId = shardId,
                        From = currentPosition + 1,
                        To = currentPosition + 50
                    });
                }

                _logger.LogInformation("Successfully recovered data on shard " + shardId);
                return true;
            }
            else
            {
                throw new Exception("Failed to fetch shard from primary.");
            }
        }

        public async void ReverseLocalTransaction(Guid shardId, string type, ShardWriteOperation operation)
        {
            var pos = _shardRepository.GetTotalShardWriteOperationsCount(shardId);
            if (operation != null)
            {
                try
                {
                    _logger.LogWarning(_nodeStateService.GetNodeLogId() + " Reverting operation " + ":" + pos + " " + operation.Operation.ToString() + " on shard " + shardId + " for object " + operation.Data.Id);
                    _shardRepository.RemoveShardWriteOperation(shardId, pos);

                }
                catch (ShardWriteOperationConcurrencyException e)
                {
                    _logger.LogError(_nodeStateService.GetNodeLogId() + "Tried to remove a sync position out of order, " + e.Message + Environment.NewLine + e.StackTrace);
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

                _shardRepository.RemoveShardWriteOperation(shardId, operation.Pos);
            }
        }
    }
}
