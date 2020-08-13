using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data.Components
{
    public class StaleDataCollector
    {
        private readonly IDataRouter _dataRouter;
        private readonly IShardRepository _shardRepository;
        private readonly ILogger _logger;
        private readonly int _deletionCacheSize = 50;

        public StaleDataCollector(
            ILogger<StaleDataCollector> logger,
            IShardRepository shardRepository,
            IDataRouter dataRouter,
            int deletionCacheSize = 50)
        {
            _logger = logger;
            _dataRouter = dataRouter;
            _shardRepository = shardRepository;
            _deletionCacheSize = deletionCacheSize;
        }

        public async Task<int> CleanUpShard(Guid shardId, int cleanUpto)
        {
            var shard = await _shardRepository.GetShardMetadataAsync(shardId);
            var allDeleteTransactions = await _shardRepository.GetShardWriteOperationsAsync(shardId, ShardOperationOptions.Delete, _deletionCacheSize * 5);
            var totalRemoved = 0;
            List<ShardWriteOperation> deletionCache = new List<ShardWriteOperation>();

            foreach (var deleteTransaction in allDeleteTransactions)
            {
                if (deleteTransaction.Pos <= cleanUpto)
                {
                    // You will delete the create request first and the delete request last
                    foreach (var transaction in await _shardRepository.GetAllObjectShardWriteOperationAsync(shardId, deleteTransaction.Data.Id))
                    {
                        //Keep the delete operation
                        _logger.LogDebug("Removing shard operation " + transaction.Key + " from shard " + shardId);
                        //await _shardRepository.RemoveShardWriteOperationAsync(shardId, transaction.Key);
                        deletionCache.Add(transaction.Value);
                        totalRemoved++;
                    }
                }

                if(deletionCache.Count >= _deletionCacheSize)
                {
                    await _shardRepository.DeleteShardWriteOperationsAsync(deletionCache);
                    deletionCache = new List<ShardWriteOperation>();
                }
            }

            if(deletionCache.Count > 0)
            {
                await _shardRepository.DeleteShardWriteOperationsAsync(deletionCache);
            }
            return totalRemoved;
        }
    }
}
