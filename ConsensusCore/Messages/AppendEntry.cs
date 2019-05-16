using ConsensusCore.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Messages
{
    public class AppendEntry<Command> where Command : BaseCommand
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int PrevLogIndex { get; set; }
        public int PrevLogTerm { get; set; }
        public List<LogEntry<Command>> Entries { get; set; } = new List<LogEntry<Command>>();
        public int LeaderCommit { get; set; }
    }
}
