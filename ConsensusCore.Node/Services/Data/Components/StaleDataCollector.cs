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
            var allDeleteTransactions = await _shardRepository.GetQueuedDeletions(shardId, cleanUpto);
            var totalRemoved = 0;
            List<string> deletionCache = new List<string>();
            List<Guid> objectsDeleted = new List<Guid>();

            foreach (var deleteTransaction in allDeleteTransactions)
            {
                deletionCache.AddRange(deleteTransaction.ShardWriteOperationIds);
                objectsDeleted.Add(deleteTransaction.Id);

                if (deletionCache.Count >= _deletionCacheSize)
                {
                    totalRemoved += deletionCache.Count;
                    await _shardRepository.DeleteShardWriteOperationsAsync(deletionCache);
                    await _shardRepository.RemoveQueuedDeletions(shardId, objectsDeleted);
                    deletionCache.Clear();
                    objectsDeleted.Clear();
                }
            }

            if(deletionCache.Count > 0)
            {
                totalRemoved += deletionCache.Count;
                await _shardRepository.DeleteShardWriteOperationsAsync(deletionCache);
                await _shardRepository.RemoveQueuedDeletions(shardId, objectsDeleted);
            }

            return totalRemoved;
        }
    }
}
