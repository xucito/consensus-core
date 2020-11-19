using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
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
            //Console.WriteLine("Trying to get shard " + shardId + "position " + syncPos);
            var filteredSWO = ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId && swo.Value.Pos == syncPos).ToList();
            if(filteredSWO.Count() == 0)
            {
                return Task.FromResult<ShardWriteOperation>(null);
            }
            //Console.WriteLine("FOUND " + filteredSWO.Count());
            return Task.FromResult(SystemExtension.Clone(filteredSWO.FirstOrDefault().Value));
        }

        public Task<ShardWriteOperation> GetShardWriteOperationAsync(string transacionId)
        {
            if (ShardWriteOperations.ContainsKey(transacionId))
            {
                return Task.FromResult(ShardWriteOperations[transacionId]);
            }
            return Task.FromResult< ShardWriteOperation>(null);
        }

        public Task<IEnumerable<ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, ShardOperationOptions option, int limit)
        {
            return Task.FromResult(ShardWriteOperations.Where(swo => swo.Value.Operation == option).Take(limit).OrderBy(swo => swo.Value.TransactionDate).Select(s => s.Value));
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

        public int GetLastShardWriteOperationPos(Guid shardId)
        {
            var sortedShardWrites = new SortedDictionary<int, ShardWriteOperation>(ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId).ToDictionary(k => k.Value.Pos, v => v.Value));
            return sortedShardWrites.Count() == 0 ? 0 : sortedShardWrites.Last().Value.Pos;
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

        public async Task<bool> DeleteShardWriteOperationsAsync(List<ShardWriteOperation> shardWriteOperations)
        {
            foreach(var operation in shardWriteOperations)
            {
                ShardWriteOperations.TryRemove(operation.Id, out _);
            }
            return true;
        }
    }
}
