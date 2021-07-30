using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using LiteDB;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Repositories
{
    public class DefaultNodeRepository : IShardWriteOperationLogRepository
    {
        LiteDatabase _db;
        ConcurrentDictionary<string, string> keyDict = new ConcurrentDictionary<string, string>();
        ConcurrentDictionary<Guid, int> _lastShardOperation = new ConcurrentDictionary<Guid, int>();
        public DefaultNodeRepository(LiteDatabase db)
        {
            _db = db;
        }

        public string GetShardName(Guid shardId)
        {
            return "_shard_" + shardId.ToString();
        }

        public Task<bool> AddShardWriteOperationAsync(Guid shardId, ShardWriteOperationLog operation)
        {
            _lastShardOperation.AddOrUpdate(shardId, operation.Pos, (key, oldValue) =>
            {
                if (oldValue < operation.Pos)
                {
                    return operation.Pos;
                }
                return oldValue;
            });
            _db.GetCollection<ShardWriteOperationLog>(GetShardName(shardId)).Insert(operation);
            return Task.FromResult(true);
        }

        public int GetLastShardWriteOperationPos(Guid shardId)
        {
            throw new NotImplementedException();
        }

        public Task<SortedDictionary<int, ShardWriteOperationLog>> GetShardWriteOperationsAsync(Guid shardId, int from, int to)
        {
            throw new NotImplementedException();
        }

        public Task<bool> RemoveShardWriteOperationAsync(Guid shardId, int pos)
        {
            throw new NotImplementedException();
        }

        public Task<bool> ShrinkTransactionLog(Guid shardId)
        {
            throw new NotImplementedException();
        }
    }
}
