using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IShardRepository
    {
        int GetTotalShardWriteOperationsCount(Guid shardId);
        bool AddShardWriteOperation(ShardWriteOperation operation);
        bool MarkShardWriteOperationApplied(string operationId);
        bool RemoveShardWriteOperation(Guid shardId, int pos);
        SortedDictionary<int, ShardWriteOperation> GetAllUnappliedOperations(Guid shardId);
        List<ShardMetadata> GetAllShardMetadata();
        //bool MarkShardWriteOperationAsApplied(Guid shardId, int pos);
        bool AddDataReversionRecord(DataReversionRecord record);
        bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId);
        bool MarkObjectForDeletion(ObjectDeletionMarker marker);
        /// <summary>
        /// You should update based on not only the operation pos but also check the objectid, type
        /// </summary>
        /// <param name="shardId"></param>
        /// <param name="operation"></param>
        /// <returns></returns>
        bool UpdateShardWriteOperation(Guid shardId, ShardWriteOperation operation);
        ShardWriteOperation GetShardWriteOperation(Guid shardId, int syncPos);
        ShardWriteOperation GetShardWriteOperation(string transacionId);
        Task<SortedDictionary<int, ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, int from, int to);
        Task<SortedDictionary<int, ShardWriteOperation>> GetAllObjectShardWriteOperationAsync(Guid shardId, Guid objectId);
        IEnumerable<ShardWriteOperation> GetAllShardWriteOperations(Guid shardId);
        bool AddShardMetadata(ShardMetadata operation);
        ShardMetadata GetShardMetadata(Guid shardId);
    }
}
