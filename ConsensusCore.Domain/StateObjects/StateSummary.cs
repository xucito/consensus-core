using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class StateSummary
    {
        public Dictionary<string, int> ShardCounts { get; set; }
        public int TotalShards { get; set; }
    }
}
