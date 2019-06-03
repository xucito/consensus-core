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
        private object currentStateLock = new object();

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
            lock (currentStateLock)
            {
                foreach (var entry in entries.OrderBy(c => c.Index))
                {
                    foreach (var command in entry.Commands)
                    {
                        CurrentState.ApplyCommand(command);
                    }
                }
            }
        }

        public Z GetCurrentState()
        {
            return CurrentState;
        }

        public bool IndexExists(string type)
        {
            return CurrentState.Indexes.ContainsKey(type);
        }

        public SharedShardMetadata[] GetShards(string type)
        {
            return CurrentState.Indexes[type].Shards.ToArray();
        }

        public SharedShardMetadata GetShard(string type,Guid shardId)
        {
            return CurrentState.Indexes[type].Shards.Where(s => s.Id == shardId).FirstOrDefault();
        }
    }
}
