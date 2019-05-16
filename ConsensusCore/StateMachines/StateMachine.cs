using ConsensusCore.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Interfaces
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">Logs used to playback the state of the cluster</typeparam>
    /// <typeparam name="Z">Object representating the current state of the cluster</typeparam>
    public class StateMachine<T, Z>
        where T : BaseCommand
        where Z : BaseState<T>, new()
    {
        public Z DefaultState { get; set; }
        public Z CurrentState { get; private set; }

        public StateMachine(List<LogEntry<T>> entries)
        {
            DefaultState = new Z();
            CurrentState = DefaultState;
        }

        public void ApplyLogToStateMachine(LogEntry<T> entry)
        {
            foreach (var command in entry.Commands)
            {
                CurrentState.ApplyCommand(command);
            }
        }

        public void ApplyLogsToStateMachine(IEnumerable<LogEntry<T>> entries)
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
    }
}
