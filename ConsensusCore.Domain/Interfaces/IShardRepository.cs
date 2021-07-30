using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IShardWriteOperationLogRepository
    {
        Task<bool> AddShardWriteOperationAsync(Guid shardId, ShardWriteOperationLog operation);
        int GetLastShardWriteOperationPos(Guid shardId);
        Task<bool> RemoveShardWriteOperationAsync(Guid shardId, int pos);
        // To should be inclusive.
        Task<SortedDictionary<int, ShardWriteOperationLog>> GetShardWriteOperationsAsync(Guid shardId, int from, int to);
        Task<bool> ShrinkTransactionLog(Guid shardId);
    }
}
