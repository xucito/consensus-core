using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Models;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IStateMachine<Z> where Z : BaseState, new()
    {
        //Set should only be used for testing
        Z CurrentState { get; set; }
        Z DefaultState { get; set; }
        void ApplySnapshotToStateMachine(Z state);
        void ApplyLogsToStateMachine(IEnumerable<LogEntry> entries);
        void ApplyLogToStateMachine(LogEntry entry);
        IEnumerable<ShardAllocationMetadata> GetAllOutOfSyncShards(Guid nodeId);
        List<ShardAllocationMetadata> GetAllPrimaryShards(Guid nodeId);
        Dictionary<Guid, Guid> GetAllPrimaryShards(string type);
        Z GetCurrentState();
        NodeInformation GetNode(Guid nodeId);
        NodeInformation GetNode(string transportAddresss);
        NodeInformation[] GetNodes();
        ShardAllocationMetadata GetShard(string type, Guid shardId);
        ShardAllocationMetadata GetShardMetadata(Guid shardId, string type);
        ShardAllocationMetadata[] GetShards(string type = null);
        ShardAllocationMetadata[] GetShards(Guid nodeId);
        bool IndexExists(string type);
        bool IsNodeContactable(Guid nodeId);
        bool IsNodeContactable(string transportUrl);
        bool IsLocked(string name);
        bool IsLockObtained(string name, Guid lockId);
        BaseTask GetRunningTask(string uniqueId);
        ConcurrentDictionary<string, Lock> GetLocks();
    }
}