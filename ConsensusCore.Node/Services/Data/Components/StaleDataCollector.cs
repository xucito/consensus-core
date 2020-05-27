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

        public StaleDataCollector(
            ILogger<StaleDataCollector> logger,
            IShardRepository shardRepository,
            IDataRouter dataRouter)
        {
            _logger = logger;
            _dataRouter = dataRouter;
            _shardRepository = shardRepository;
        }

        public async Task<int> CleanUpShard(Guid shardId, int cleanUpto)
        {
            var shard = await _shardRepository.GetShardMetadataAsync(shardId);
            var allDeleteTransactions = await _shardRepository.GetShardWriteOperationsAsync(ShardOperationOptions.Delete);
            var totalRemoved = 0;

            foreach(var deleteTransaction in allDeleteTransactions)
            {
                if (deleteTransaction.Pos < cleanUpto)
                {
                    // You will delete the create request first and the delete request last
                    foreach (var transaction in await _shardRepository.GetAllObjectShardWriteOperationAsync(shardId, deleteTransaction.Data.Id))
                    {
                            _logger.LogDebug("Removing shard operation " + transaction.Key + " from shard " + shardId);
                            await _shardRepository.RemoveShardWriteOperationAsync(shardId, transaction.Key);
                            totalRemoved++;
                    }
                }
            }
            return totalRemoved;
        }
    }
}
