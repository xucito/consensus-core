using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.RPCs
{
    public class AppendEntry : BaseRequest<AppendEntryResponse>
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int PrevLogIndex { get; set; }
        public int PrevLogTerm { get; set; }
        public List<LogEntry> Entries { get; set; } = new List<LogEntry>();
        public int LeaderCommit { get; set; }

        public override string RequestName => "AppendEntry";
    }

    public class AppendEntryResponse
    {
        public bool Successful { get; set; }
        public string ConflictName { get; set; }
        public int? ConflictingTerm { get; set; }
        public int? FirstTermIndex { get; set; }
        public int? LastLogEntryIndex { get; set; }
    }
}
