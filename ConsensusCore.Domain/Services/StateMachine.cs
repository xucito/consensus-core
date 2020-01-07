using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Utility;
using Microsoft.Extensions.Logging;
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
        private Z _currentState;
        public Z DefaultState { get; set; }
        public Z CurrentState{
            get;
            private set;
        }
        public int CommitIndex { get; set; }
        public int CurrentTerm { get; set; }
        public Guid Id { get; set; }
        private ILogger _logger;
        private bool _disabledLogging = true;

        private object currentStateLock = new object();

        public StateMachine(ILogger<StateMachine<Z>> logger)
        {
            DefaultState = new Z();
            CurrentState = DefaultState;
            _logger = logger;
            _disabledLogging = false;
        }

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
                    lock (currentStateLock)
                    {
                        _currentState.ApplyCommand(command);
                    }
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
            foreach (var entry in copy)
            {
                foreach (var command in entry.Commands)
                {
                    try
                    {
                        if (!_disabledLogging)
                            _logger.LogDebug("Applying command " + Environment.NewLine + JsonConvert.SerializeObject(command, Formatting.Indented));

                        lock (currentStateLock)
                        {
                            _currentState.ApplyCommand(command);
                        }
                        if (!_disabledLogging)
                            _logger.LogDebug("State " + Environment.NewLine + JsonConvert.SerializeObject(CurrentState, Formatting.Indented));
                    }
                    catch (Exception e)
                    {
                        if (!_disabledLogging)
                            _logger.LogError("Failed to apply entry with message: " + e.Message + Environment.NewLine + e.StackTrace);
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

        public ShardAllocationMetadata[] GetShards(string type = null)
        {
            if (type == null)
            {
                return CurrentState.Indexes.SelectMany(i => i.Value.Shards).ToArray();
            }
            return CurrentState.Indexes[type].Shards.ToArray();
        }

        public ShardAllocationMetadata[] GetShards(Guid nodeId)
        {
            return CurrentState.Indexes.SelectMany(i => i.Value.Shards.Where(n => n.InsyncAllocations.Contains(nodeId) || n.StaleAllocations.Contains(nodeId))).ToArray();
        }

        public ShardAllocationMetadata GetShard(string type, Guid shardId)
        {
            return CurrentState.Indexes[type].Shards.Where(s => s.Id == shardId).FirstOrDefault();
        }

        public List<ShardAllocationMetadata> GetAllPrimaryShards(Guid nodeId)
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

        public ShardAllocationMetadata GetShardMetadata(Guid shardId, string type)
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
        public IEnumerable<ShardAllocationMetadata> GetAllOutOfSyncShards(Guid nodeId)
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
