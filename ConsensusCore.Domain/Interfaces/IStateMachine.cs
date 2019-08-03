using System;
using System.Collections.Generic;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IStateMachine<Z> where Z : BaseState, new()
    {
        Z CurrentState { get; }
        Z DefaultState { get; set; }

        void ApplyLogsToStateMachine(IEnumerable<LogEntry> entries);
        void ApplyLogToStateMachine(LogEntry entry);
        IEnumerable<SharedShardMetadata> GetAllOutOfSyncShards(Guid nodeId);
        List<SharedShardMetadata> GetAllPrimaryShards(Guid nodeId);
        Dictionary<Guid, Guid> GetAllPrimaryShards(string type);
        Z GetCurrentState();
        NodeInformation GetNode(Guid nodeId);
        NodeInformation GetNode(string transportAddresss);
        NodeInformation[] GetNodes();
        SharedShardMetadata GetShard(string type, Guid shardId);
        SharedShardMetadata GetShardMetadata(Guid shardId, string type);
        SharedShardMetadata[] GetShards(string type = null);
        SharedShardMetadata[] GetShards(Guid nodeId);
        bool IndexExists(string type);
        bool IsNodeContactable(Guid nodeId);
        bool IsNodeContactable(string transportUrl);
        bool IsObjectLocked(Guid objectId);
        BaseTask GetRunningTask(string uniqueId);
    }
}