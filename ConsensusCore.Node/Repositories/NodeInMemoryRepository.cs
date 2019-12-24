using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ConsensusCore.Node.Repositories
{
    public class NodeInMemoryRepository<Z> : IBaseRepository<Z>, IShardRepository
        where Z : BaseState, new()
    {
        public ConcurrentBag<DataReversionRecord> DataReversionRecords { get; set; } = new ConcurrentBag<DataReversionRecord>();
        public ConcurrentDictionary<Guid, LocalShardMetaData> LocalShardMetaDatas { get; set; } = new ConcurrentDictionary<Guid, LocalShardMetaData>();
        public ConcurrentDictionary<string, ShardOperation> ShardOperations { get; set; } = new ConcurrentDictionary<string, ShardOperation>();
        public ConcurrentBag<ObjectDeletionMarker> ObjectDeletionMarker { get; set; } = new ConcurrentBag<ObjectDeletionMarker>();

        public NodeInMemoryRepository()
        {
        }

        public bool AddDataReversionRecord(DataReversionRecord record)
        {
            DataReversionRecords.Add(SystemExtension.Clone(record));
            return true;
        }

        public bool AddShardMetadata(LocalShardMetaData shardMetadata)
        {
            LocalShardMetaDatas.TryAdd(shardMetadata.ShardId, SystemExtension.Clone(shardMetadata));
            return true;
        }

        public bool AddShardOperation(ShardOperation operation)
        {
            ShardOperations.TryAdd(operation.ShardId + ":" + operation.Pos, SystemExtension.Clone(operation));
            return true;
        }

        public ShardOperation GetShardOperation(Guid shardId, int pos)
        {
            if (ShardOperations.ContainsKey(shardId + ":" + pos))
                return SystemExtension.Clone(ShardOperations[shardId + ":" + pos]);
            return null;
        }

        public LocalShardMetaData GetShardMetadata(Guid shardId)
        {
            return SystemExtension.Clone(LocalShardMetaDatas[shardId]);
        }

        public int GetTotalShardOperationsCount(Guid shardId)
        {
            return ShardOperations.Count();
        }

        public bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId)
        {
            return ObjectDeletionMarker.Where(odm => odm.ObjectId == objectId && odm.ShardId == shardId).Count() > 0;
        }

        public NodeStorage<Z> LoadNodeData()
        {
            return new NodeStorage<Z>()
            {
                Id = Guid.NewGuid()
            };
        }

        public bool MarkObjectForDeletion(ObjectDeletionMarker marker)
        {
            ObjectDeletionMarker.Add(SystemExtension.Clone(marker));
            return true;
        }

        /* public bool MarkShardOperationAsApplied(Guid shardId, int pos)
         {
             var shardOperation = ShardOperations.Where(lsm => lsm.ShardId == shardId && lsm.Pos == pos).FirstOrDefault();
             if(shardOperation == null)
             {
                 throw new Exception("Failed to mark shard operation as applied as shard is missing");
             }
             shardOperation.Applied = true;
             return true;
         }

         public bool MarkShardOperationAsCommited(Guid shardId, int syncPos)
         {
             LocalShardMetaDatas.Where(lsm => lsm.ShardId == shardId).First().SyncPos = syncPos;
             return true;
         }*/


        public bool RemoveShardOperation(Guid shardId, int pos)
        {
            return ShardOperations.TryRemove(shardId + ":" + pos, out _);
        }

        public void SaveNodeData(NodeStorage<Z> storage)
        {
        }

        public bool ShardMetadataExists(Guid shardId)
        {
            return LocalShardMetaDatas.ContainsKey(shardId);
        }

        public bool UpdateShardMetadata(LocalShardMetaData shardMetadata)
        {
            LocalShardMetaDatas[shardMetadata.ShardId] = SystemExtension.Clone(shardMetadata);
            return true;
        }

        public IEnumerable<ShardOperation> GetAllUncommitedOperations(Guid shardId)
        {
            return SystemExtension.Clone(ShardOperations.Where(so => so.Value.Applied == false && so.Value.ShardId == shardId).Select(s => s.Value));
        }

        public IEnumerable<ShardOperation> GetAllShardOperations(Guid shardId)
        {
            return SystemExtension.Clone(ShardOperations.Select(so => so.Value));
        }

        public bool UpdateShardOperation(Guid shardId, ShardOperation operation)
        {
            ShardOperations[shardId + ":" + operation.Pos] = SystemExtension.Clone(operation);
            return true;
        }
    }
}
