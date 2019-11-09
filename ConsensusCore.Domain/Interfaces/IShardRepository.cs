using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IShardRepository
    {
        int GetTotalShardOperationsCount(Guid shardId);
        bool AddShardOperation(ShardOperation operation);
        bool RemoveShardOperation(Guid shardId, int pos);
        //bool MarkShardOperationAsApplied(Guid shardId, int pos);
        bool AddDataReversionRecord(DataReversionRecord record);
        bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId);
        bool MarkObjectForDeletion(ObjectDeletionMarker marker);
        bool AddShardMetadata(LocalShardMetaData shardMetadata);
        bool UpdateShardMetadata(LocalShardMetaData shardMetadata);
        /// <summary>
        /// You should update based on not only the operation pos but also check the objectid, type
        /// </summary>
        /// <param name="shardId"></param>
        /// <param name="operation"></param>
        /// <returns></returns>
        bool UpdateShardOperation(Guid shardId, ShardOperation operation);
        LocalShardMetaData GetShardMetadata(Guid shardId);
        bool ShardMetadataExists(Guid shardId);
        ShardOperation GetShardOperation(Guid shardId, int syncPos);
        IEnumerable<ShardOperation> GetAllShardOperations(Guid shardId);
        //bool MarkShardOperationAsCommited(Guid shardId, int syncPos);
        IEnumerable<ShardOperation> GetAllUncommitedOperations(Guid shardId);
    }
}
