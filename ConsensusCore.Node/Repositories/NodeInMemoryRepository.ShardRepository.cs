using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Repositories
{
    public partial class NodeInMemoryRepository<Z> : IShardRepository
        where Z : BaseState, new()
    {
        public Task<bool> AddDataReversionRecordAsync(DataReversionRecord record)
        {
            DataReversionRecords.Add(SystemExtension.Clone(record));
            return Task.FromResult(true);
        }

        public Task<bool> AddShardMetadataAsync(ShardMetadata shardMetadata)
        {
            ShardMetadata.Add(shardMetadata.ShardId, shardMetadata);
            return Task.FromResult(true);
        }

        public Task<bool> AddShardWriteOperationAsync(ShardWriteOperation operation)
        {
            return Task.FromResult(ShardWriteOperations.TryAdd(operation.Id, SystemExtension.Clone(operation)));
        }

        public Task<SortedDictionary<int, ShardWriteOperation>> GetAllObjectShardWriteOperationAsync(Guid shardId, Guid objectId)
        {
            var result = new SortedDictionary<int, ShardWriteOperation>();
            foreach (var operation in ShardWriteOperations.Where(so => so.Value.Data.Id == objectId && so.Value.Data.ShardId == shardId))
            {
                result.Add(operation.Value.Pos, operation.Value);
            }
            return Task.FromResult(result);
        }

        public Task<List<ShardMetadata>> GetAllShardMetadataAsync()
        {
            return Task.FromResult(ShardMetadata.Select(sm => sm.Value).ToList());
        }

        public Task<IEnumerable<ShardWriteOperation>> GetAllShardWriteOperationsAsync(Guid shardId)
        {
            return Task.FromResult(SystemExtension.Clone(ShardWriteOperations.Select(so => so.Value)));
        }

        public Task<SortedDictionary<int, ShardWriteOperation>> GetAllUnappliedOperationsAsync(Guid shardId)
        {
            var sortedSWO = ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId).ToList();
            SortedDictionary<int, ShardWriteOperation> result = new SortedDictionary<int, ShardWriteOperation>();
            foreach (var operation in sortedSWO)
            {
                result.Add(operation.Value.Pos, operation.Value);
            }
            return Task.FromResult(result);
        }

        public Task<ShardMetadata> GetShardMetadataAsync(Guid shardId)
        {
            return Task.FromResult(ShardMetadata.GetValueOrDefault(shardId));
        }

        public Task<ShardWriteOperation> GetShardWriteOperationAsync(Guid shardId, int syncPos)
        {
            return Task.FromResult(SystemExtension.Clone(ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId && swo.Value.Pos == syncPos).FirstOrDefault().Value));
        }

        public Task<ShardWriteOperation> GetShardWriteOperationAsync(string transacionId)
        {
            if (ShardWriteOperations.ContainsKey(transacionId))
            {
                return Task.FromResult(ShardWriteOperations[transacionId]);
            }
            return Task.FromResult< ShardWriteOperation>(null);
        }

        public Task<SortedDictionary<int, ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, int from, int to)
        {
            var writes = ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId && swo.Value.Pos >= from && swo.Value.Pos <= to);
            SortedDictionary<int, ShardWriteOperation> operations = new SortedDictionary<int, ShardWriteOperation>();
            foreach (var write in writes)
            {
                operations.Add(write.Value.Pos, write.Value);
            }
            return Task.FromResult(operations);
        }

        public int GetTotalShardWriteOperationsCount(Guid shardId)
        {
            return ShardWriteOperations.Count();
        }

        public bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId)
        {
            return ObjectDeletionMarker.Where(odm => odm.ObjectId == objectId && odm.ShardId == shardId).Count() > 0;
        }

        public Task<bool> MarkObjectForDeletionAsync(ObjectDeletionMarker marker)
        {
            ObjectDeletionMarker.Add(SystemExtension.Clone(marker));
            return Task.FromResult(true);
        }

        public Task<bool> MarkShardWriteOperationAppliedAsync(string operationId)
        {
            if (ShardWriteOperations.ContainsKey(operationId))
                return Task.FromResult(ShardWriteOperations[operationId].Applied = true);
            return Task.FromResult(false);
        }

        public async Task<bool> RemoveShardWriteOperationAsync(Guid shardId, int pos)
        {
            return ShardWriteOperations.TryRemove((await GetShardWriteOperationAsync(shardId, pos)).Id, out _);
        }
    }
}
