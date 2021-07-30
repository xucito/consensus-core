using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using LiteDB;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Db
{
    public class LiteDbRepository : IShardRepository
    {
        LiteDatabase _db;
        ConcurrentDictionary<string, string> keyDict = new ConcurrentDictionary<string, string>();
        public LiteDbRepository(LiteDatabase db)
        {
            _db = db;
        }

        ConcurrentDictionary<Guid, int> _lastShardOperation = new ConcurrentDictionary<Guid, int>();

        public string GetShardName(Guid shardId)
        {
            return "_shard_" + shardId.ToString();
        }

        public string NormalizeCollectionString(Type type)
        {
            if (!keyDict.ContainsKey(type.Name))
            {
                keyDict.TryAdd(type.Name, Regex.Replace(type.Name, @"[^0-9a-zA-Z:,]+", "_"));
            }

            return keyDict[type.Name];
        }

        public int GetLastShardWriteOperationPos(Guid shardId)
        {
            int lastValue;

            if (_lastShardOperation.TryGetValue(shardId, out lastValue))
            {
                return lastValue;
            }

            var lastOperation = _db.GetCollection<ShardWriteOperation>(GetShardName(shardId)).Query().OrderByDescending(swo => swo.Pos).FirstOrDefault();
            if (lastOperation != null)
                return (int)lastOperation.Pos;
            else
                return 0;
        }

        public Task<bool> AddShardWriteOperationAsync(Guid shardId, ShardWriteOperation operation)
        {
            _lastShardOperation.AddOrUpdate(shardId, operation.Pos, (key, oldValue) =>
            {
                if (oldValue < operation.Pos)
                {
                    return operation.Pos;
                }
                return oldValue;
            });
            _db.GetCollection<ShardWriteOperation>(GetShardName(shardId)).Insert(operation);
            return Task.FromResult(true);
        }

        public Task<bool> RemoveShardWriteOperationAsync(Guid shardId, int pos)
        {
            var executed = _db.GetCollection<ShardWriteOperation>(GetShardName(shardId)).DeleteMany(swo => swo.Pos == pos);
            if(executed > 0)
            {
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        public Task<ShardWriteOperation> GetShardWriteOperationAsync(Guid shardId, int syncPos)
        {
            throw new NotImplementedException();
        }

        public Task<ShardWriteOperation> GetShardWriteOperationAsync(Guid shardId, string transacionId)
        {
            throw new NotImplementedException();
        }

        public Task<SortedDictionary<int, ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, int from, int to)
        {
            throw new NotImplementedException();
        }

        public Task<SortedDictionary<int, ShardWriteOperation>> GetAllObjectShardWriteOperationAsync(Guid shardId, Guid objectId)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ShardWriteOperation>> GetShardWriteOperationsAsync(Guid shardId, ShardOperationOptions option, int limit)
        {
            throw new NotImplementedException();
        }

        public Task<bool> DeleteShardWriteOperationsAsync(Guid shardId, List<string> shardWriteOperations)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ShardWriteOperation>> GetAllShardWriteOperationsAsync(Guid shardId)
        {
            throw new NotImplementedException();
        }

        public Task<bool> AddShardMetadataAsync(ShardMetadata shardMetadata)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ObjectDeletionMarker>> GetQueuedDeletions(Guid shardId, int toPos)
        {
            throw new NotImplementedException();
        }

        public Task<bool> MarkObjectForDeletionAsync(ObjectDeletionMarker marker)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveQueuedDeletions(Guid shardId, List<Guid> objectIds)
        {
            throw new NotImplementedException();
        }
    }
}
