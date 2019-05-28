using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Interfaces
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">Logs used to playback the state of the cluster</typeparam>
    /// <typeparam name="Z">Object representating the current state of the cluster</typeparam>
    public class StateMachine<Z>
        where Z : BaseState, new()
    {
        public Z DefaultState { get; set; }
        public Z CurrentState { get; private set; }

        public StateMachine()
        {
            DefaultState = new Z();
            CurrentState = DefaultState;
        }

        public void ApplyLogToStateMachine(LogEntry entry)
        {
            foreach (var command in entry.Commands)
            {
                CurrentState.ApplyCommand(command);
            }
        }

        public void ApplyLogsToStateMachine(IEnumerable<LogEntry> entries)
        {
            foreach (var entry in entries.OrderBy(c => c.Index))
            {
                foreach (var command in entry.Commands)
                {
                    CurrentState.ApplyCommand(command);
                }
            }
        }

        public Z GetCurrentState()
        {
            return CurrentState;
        }

        public bool ShardIsAssignedToNode(Guid shardId, Guid nodeId)
        {
            return CurrentState.Shards[shardId].Allocations.ContainsKey(nodeId);
        }

        public bool ShardIsPrimaryOnNode(Guid shardId, Guid nodeId)
        {
            return CurrentState.Shards[shardId].PrimaryAllocation == nodeId;
        }

        public bool ShardExists(Guid shardId)
        {
            return CurrentState.Shards.ContainsKey(shardId);
        }

        public bool NodeHasOlderShard(Guid nodeId, Guid shardId, int newVersion)
        {
            return CurrentState.Shards[shardId].Allocations[nodeId] < newVersion;
        }

        public Guid? GetShardContainingObject(Guid objectId, string type)
        {
            var shards = CurrentState.Shards.Where(s => s.Value.DataTable.ContainsKey(objectId)).Select(s => s.Key);

            if(shards.Count() == 0)
            {
                return null;
            }
            else
            {
                return shards.First();
            }
        }

        public bool NodeHasShardLatestVersion(Guid nodeId, Guid shardId)
        {
            return AllNodesWithUptoDateShard(shardId).Contains(nodeId);
        }

        public Guid GetShardPrimaryNode(Guid shardId)
        {
            return CurrentState.Shards[shardId].PrimaryAllocation;
        }

        public Guid[] AllNodesWithUptoDateShard(Guid shardId)
        {
            var latestVersion = CurrentState.Shards[shardId].Version;
            return CurrentState.Shards[shardId].Allocations.Where(a => a.Value == latestVersion).Select(s => s.Key).ToArray();
        }

        public int? GetLatestShardVersion(Guid shardId)
        {
            return CurrentState.Shards[shardId].Version;
        }

        public bool ShardTypeExists(string type)
        {
            return CurrentState.Shards.Where(s => s.Value.Type == type).Count() > 0;
        }

        public bool WritableShardExists(string type, out ShardMetadata lastShard)
        {
            var latestShard = CurrentState.Shards.Where(s => s.Value.Type == type).OrderByDescending(s => s.Value.ShardNumber);
            if (latestShard.Count() == 0)
            {
                lastShard = null;
                return false;
            }
            lastShard = latestShard.First().Value;
            return latestShard.First().Value.DataTable.Count() < latestShard.First().Value.MaxSize;
        }

        public int TotalShards { get { return CurrentState.Shards.Count(); } }
    }
}
