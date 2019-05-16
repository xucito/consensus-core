using ConsensusCore.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.ViewModels
{
    public class NodeInfo
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public double Version { get; set; }
        public int CurrentTerm { get; set; } = 0;
        public Guid? VotedFor { get; set; } = null;
    }

    public class NodeInfo<T> : NodeInfo where T : BaseCommand
    {
        public List<LogEntry<T>> Logs { get; } = new List<LogEntry<T>>();

        public NodeInfo(List<LogEntry<T>> logs)
        {
            Logs = logs;
        }

        public LogEntry<T> GetLogAtIndex(int i)
        {
            return Logs.Where(l => l.Index == i).FirstOrDefault();
        }

        /// <summary>
        /// Index at 0 is always true
        /// </summary>
        /// <param name="index"></param>
        /// <param name="term"></param>
        /// <returns></returns>
        public bool DoesLogExist(int index, int term)
        {
            if(index == 0)
            {
                return true;
            }
            else
            {
                if(Logs.Where(l => l.Index == index && l.Term == term).Count() > 0)
                {
                    return true;
                }
                return false;
            }
        }

        public void AddLogs(List<LogEntry<T>> logs)
        {
            foreach(var log in logs)
            {
                if(Logs.Where(l => l.Index == log.Index && l.Term == log.Term).Count() == 0)
                {
                    Logs.Add(log);
                }
            }
        }
    }
}
