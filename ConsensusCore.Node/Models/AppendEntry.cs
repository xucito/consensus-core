using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Models
{
    public class AppendEntry<TCommand> where TCommand : BaseCommand
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int PrevLogIndex { get; set; }
        public int PrevLogTerm { get; set; }
        public List<LogEntry<TCommand>> Entries { get; set; } = new List<LogEntry<TCommand>>();
        public int LeaderCommit { get; set; }
    }
}
