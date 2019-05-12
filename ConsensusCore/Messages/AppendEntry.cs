using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Messages
{
    public class AppendEntry
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int PrevLogIndex { get; set; }
        public List<string> Entries { get; set; }
        public int LeaderCommit { get; set; }
        public Guid NodeId { get; set; }
    }
}
