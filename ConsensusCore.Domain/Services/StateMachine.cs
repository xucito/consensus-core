using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Services
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">Logs used to playback the state of the cluster</typeparam>
    /// <typeparam name="Z">Object representating the current state of the cluster</typeparam>
    public class StateMachine<Z> : IStateMachine<Z>
        where Z : BaseState, new()
    {
        public Z DefaultState { get; set; }
        public Z CurrentState { get; private set; }
        public int CommitIndex { get; set; }
        public int CurrentTerm { get; set; }
        public Guid Id { get; set; }

        private object currentStateLock = new object();

        public StateMachine()
        {
            DefaultState = new Z();
            CurrentState = DefaultState;
        }

        public void ApplyLogToStateMachine(LogEntry entry)
        {
            foreach (var command in entry.DeepCopy().Commands)
            {
                try
                {
                    CurrentState.ApplyCommand(command);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        public void ApplyLogsToStateMachine(IEnumerable<LogEntry> entries)
        {
            List<string> FailedLogs = new List<string>();
            var copy = entries.OrderBy(c => c.Index).Select(e => e.DeepCopy());
            lock (currentStateLock)
            {
                foreach (var entry in copy)
                {
                    foreach (var command in entry.Commands)
                    {
                        try
                        {
                            CurrentState.ApplyCommand(command);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }
                    }
                }
            }

            /*if(FailedLogs.Count() > 0)
            {
                throw new Exception("Failed to apply all commands successfully, the following logs failed to apply to state" + Environment.NewLine + JsonConvert.SerializeObject(FailedLogs));
            }*/
        }

        public void ApplySnapshotToStateMachine(Z state)
        {
            this.CurrentState = state;
        }

        public Z GetCurrentState()
        {
            return CurrentState;
        }

        public bool IndexExists(string type)
        {
            return CurrentState.Indexes.ContainsKey(type);
        }

        public SharedShardMetadata[] GetShards(string type = null)
        {
            if (type == null)
            {
                return CurrentState.Indexes.SelectMany(i => i.Value.Shards).ToArray();
            }
            return CurrentState.Indexes[type].Shards.ToArray();
        }

        public SharedShardMetadata[] GetShards(Guid nodeId)
        {
            return CurrentState.Indexes.SelectMany(i => i.Value.Shards.Where(n => n.InsyncAllocations.Contains(nodeId) || n.StaleAllocations.Contains(nodeId))).ToArray();
        }

        public SharedShardMetadata GetShard(string type, Guid shardId)
        {
            return CurrentState.Indexes[type].Shards.Where(s => s.Id == shardId).FirstOrDefault();
        }

        public List<SharedShardMetadata> GetAllPrimaryShards(Guid nodeId)
        {
            return CurrentState.Indexes.SelectMany(i => i.Value.Shards.Where(s => s.PrimaryAllocation == nodeId)).ToList();
        }

        public NodeInformation GetNode(Guid nodeId)
        {
            var nodes = CurrentState.Nodes.Where(n => n.Key == nodeId);
            if (nodes.Count() == 0)
            {
                return null;
            }
            else
            {
                return nodes.First().Value;
            }
        }

        public NodeInformation[] GetNodes()
        {
            return CurrentState.Nodes.Values.ToArray();
        }

        public NodeInformation GetNode(string transportAddresss)
        {
            var nodes = CurrentState.Nodes.Where(n => n.Value.TransportAddress == transportAddresss);
            if (nodes.Count() == 0)
            {
                return null;
            }
            else
            {
                return nodes.First().Value;
            }
        }

        /// <summary>
        /// Get all the primary shards for a given type
        /// Id, allocation
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public Dictionary<Guid, Guid> GetAllPrimaryShards(string type)
        {
            return CurrentState.Indexes[type].Shards.ToDictionary(k => k.Id, v => v.PrimaryAllocation);
        }

        public SharedShardMetadata GetShardMetadata(Guid shardId, string type)
        {
            return CurrentState.Indexes[type].Shards.Where(s => s.Id == shardId).FirstOrDefault();
        }

        public bool IsNodeContactable(Guid nodeId)
        {
            if (CurrentState.Nodes.ContainsKey(nodeId))
            {
                return CurrentState.Nodes[nodeId].IsContactable;
            }
            return false;
        }

        public bool IsNodeContactable(string transportUrl)
        {
            var foundNodes = CurrentState.Nodes.Where(n => n.Value.TransportAddress == transportUrl);
            if (foundNodes.Count() == 1)
            {
                return foundNodes.First().Value.IsContactable;
            }
            return false;
        }

        public bool IsObjectLocked(Guid objectId)
        {
            return CurrentState.ObjectLocks.ContainsKey(objectId);
        }



        public bool IsLockObtained(Guid objectId, Guid lockId)
        {
            return CurrentState.ObjectLocks.ContainsKey(objectId) && CurrentState.ObjectLocks[objectId].LockId == lockId;
        }

        /// <summary>
        /// List of shard ids and types that are out of sync for the given node
        /// </summary>
        public IEnumerable<SharedShardMetadata> GetAllOutOfSyncShards(Guid nodeId)
        {
            return CurrentState.Indexes.SelectMany(i => i.Value.Shards.Where(s => s.StaleAllocations.Contains(nodeId)));
        }

        public BaseTask GetRunningTask(string uniqueId)
        {
            return CurrentState.GetRunningTask(uniqueId);
        }

        public ConcurrentDictionary<Guid, ObjectLock> GetObjectLocks()
        {
            return new ConcurrentDictionary<Guid, ObjectLock>(CurrentState.ObjectLocks);
        }

        public void ApplySnapshotToStateMachine(BaseState state)
        {
            throw new NotImplementedException();
        }
    }
}
