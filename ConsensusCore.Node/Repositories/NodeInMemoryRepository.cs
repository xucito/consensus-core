using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Repositories
{
    public class NodeInMemoryRepository<Z> : IBaseRepository<Z>, IShardRepository, IOperationCacheRepository
        where Z : BaseState, new()
    {
        public ConcurrentBag<DataReversionRecord> DataReversionRecords { get; set; } = new ConcurrentBag<DataReversionRecord>();
        public ConcurrentDictionary<string, ShardWriteOperation> ShardWriteOperations { get; set; } = new ConcurrentDictionary<string, ShardWriteOperation>();
        public ConcurrentBag<ObjectDeletionMarker> ObjectDeletionMarker { get; set; } = new ConcurrentBag<ObjectDeletionMarker>();
        public List<ShardWriteOperation> OperationQueue { get; set; } = new List<ShardWriteOperation>();
        public Dictionary<string, ShardWriteOperation> TransitQueue { get; set; } = new Dictionary<string, ShardWriteOperation>();
        public Dictionary<Guid, ShardMetadata> ShardMetadata { get; set; } = new Dictionary<Guid, ShardMetadata>();
        public object queueLock = new object();


        public NodeInMemoryRepository()
        {
        }

        public bool AddDataReversionRecord(DataReversionRecord record)
        {
            DataReversionRecords.Add(SystemExtension.Clone(record));
            return true;
        }

        public bool AddShardWriteOperation(ShardWriteOperation operation)
        {
            ShardWriteOperations.TryAdd(operation.Id, SystemExtension.Clone(operation));
            return true;
        }

        public ShardWriteOperation GetShardWriteOperation(Guid shardId, int pos)
        {
            return SystemExtension.Clone(ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId && swo.Value.Pos == pos).FirstOrDefault().Value);
        }

        public int GetTotalShardWriteOperationsCount(Guid shardId)
        {
            return ShardWriteOperations.Count();
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

        /* public bool MarkShardWriteOperationAsApplied(Guid shardId, int pos)
         {
             var shardOperation = ShardWriteOperations.Where(lsm => lsm.ShardId == shardId && lsm.Pos == pos).FirstOrDefault();
             if(shardOperation == null)
             {
                 throw new Exception("Failed to mark shard operation as applied as shard is missing");
             }
             shardOperation.Applied = true;
             return true;
         }

         public bool MarkShardWriteOperationAsCommited(Guid shardId, int syncPos)
         {
             LocalShardMetaDatas.Where(lsm => lsm.ShardId == shardId).First().SyncPos = syncPos;
             return true;
         }*/


        public bool RemoveShardWriteOperation(Guid shardId, int pos)
        {
            return ShardWriteOperations.TryRemove(GetShardWriteOperation(shardId, pos).Id, out _);
        }

        public bool RemoveShardWriteOperation(string transactionId)
        {
            return ShardWriteOperations.TryRemove(transactionId, out _);
        }

        public void SaveNodeData(NodeStorage<Z> storage)
        {
        }

        public IEnumerable<ShardWriteOperation> GetAllShardWriteOperations(Guid shardId)
        {
            return SystemExtension.Clone(ShardWriteOperations.Select(so => so.Value));
        }

        public bool UpdateShardWriteOperation(Guid shardId, ShardWriteOperation operation)
        {
            ShardWriteOperations[operation.Id] = SystemExtension.Clone(operation);
            return true;
        }

        public bool EnqueueOperation(ShardWriteOperation data)
        {
            OperationQueue.Add(data);
            return true;
        }

        public ShardWriteOperation GetNextOperation()
        {
            var result = OperationQueue.Take(1);
            if (result.Count() > 0)
            {
                return result.First();
            }
            return null;
        }

        public bool DeleteOperationFromTransit(string transactionId)
        {
            return TransitQueue.Remove(transactionId);
        }

        public int CountOperationsInQueue()
        {
            return OperationQueue.Count();
        }

        public int CountOperationsInTransit()
        {
            return TransitQueue.Count();
        }

        public bool DeleteOperationFromQueue(ShardWriteOperation operation)
        {
            lock (queueLock)
            {
                OperationQueue.Remove(operation);
            }

            return true;
        }

        public bool AddOperationToTransit(ShardWriteOperation operation)
        {
            return TransitQueue.TryAdd(operation.Id, operation);
        }

        public bool IsOperationInTransit(string operationId)
        {
            return TransitQueue.ContainsKey(operationId);
        }

        public SortedDictionary<int, ShardWriteOperation> GetAllObjectShardWriteOperation(Guid shardId, Guid objectId)
        {
            var result = new SortedDictionary<int, ShardWriteOperation>();
            foreach (var operation in ShardWriteOperations.Where(so => so.Value.Data.Id == objectId && so.Value.Data.ShardId == shardId))
            {
                result.Add(operation.Value.Pos, operation.Value);
            }
            return result;
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

        public Task<SortedDictionary<int, ShardWriteOperation>> GetAllObjectShardWriteOperationAsync(Guid shardId, Guid objectId)
        {
            var writes = ShardWriteOperations.Where(swo => swo.Value.Data.ShardId == shardId && swo.Value.Data.Id == objectId);
            SortedDictionary<int, ShardWriteOperation> operations = new SortedDictionary<int, ShardWriteOperation>();
            foreach (var write in writes)
            {
                operations.Add(write.Value.Pos, write.Value);
            }
            return Task.FromResult(operations);
        }

        public bool AddShardMetadata(ShardMetadata shardMetadata)
        {
            ShardMetadata.Add(shardMetadata.ShardId, shardMetadata);
            return true;
        }

        public ShardMetadata GetShardMetadata(Guid shardId)
        {
            return ShardMetadata.GetValueOrDefault(shardId);
        }

        public bool IsOperationInQueue(string operationId)
        {
            return OperationQueue.ToList().Find(oq => oq.Id == operationId) != null;
        }

        public ShardWriteOperation GetShardWriteOperation(string transacionId)
        {
            return ShardWriteOperations[transacionId];
        }
    }
}
