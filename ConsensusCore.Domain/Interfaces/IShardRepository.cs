using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IShardRepository
    {
        bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId);
        /// <summary>
        /// Last shard operation position
        /// </summary>
        /// <param name="shardId"></param>
        /// <returns></returns>
        //int GetTotalShardWriteOperationsCount(Guid shardId);
        int GetLastShardWriteOperationPos(Guid shardId);
        Task<bool> AddShardWriteOperationAsync(ShardWriteOperation operation);
        //Task<bool> MarkShardWriteOperationAppliedAsync(string operationId);
        Task<bool> RemoveShardWriteOperationAsync(Guid shardId, int pos);
        //Task<SortedDictionary<int, ShardWriteOperation>> GetAllUnappliedOperationsAsync(Guid shardId);
        Task<List<ShardMetadata>> GetAllShardMetadataAsync();
        Task<bool> AddDataReversionRecordAsync(DataReversionRecord record);
        Task<bool> MarkObjectForDeletionAsync(ObjectDeletionMarker marker);
        Task<ShardWriteOperation> GetShardWriteOperationAsync(Guid shardId, int syncPos);
        Task<ShardWriteOperation> GetShardWriteOperationAsync(string transacionId);
        // To should be inclusive.
        Task<SortedDictionary<int, ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, int from, int to);
        Task<SortedDictionary<int, ShardWriteOperation>> GetAllObjectShardWriteOperationAsync(Guid shardId, Guid objectId);
        /// <summary>
        /// Get a certain amount of shard records ordered by createdOn Date
        /// </summary>
        /// <param name="shardId"></param>
        /// <param name="option"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        Task<List<ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, ShardOperationOptions option, int limit);
        /// <summary>
        /// Must be ACID transaction
        /// </summary>
        /// <param name="shardWriteOperations"></param>
        /// <returns></returns>
        Task<bool> DeleteShardWriteOperationsAsync(List<ShardWriteOperation> shardWriteOperations);
        Task<IEnumerable<ShardWriteOperation>> GetAllShardWriteOperationsAsync(Guid shardId);
        Task<bool> AddShardMetadataAsync(ShardMetadata shardMetadata);
        Task<ShardMetadata> GetShardMetadataAsync(Guid shardId);
    }
}
