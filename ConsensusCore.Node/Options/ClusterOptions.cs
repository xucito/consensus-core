using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node
{
    public class ClusterOptions
    {
        public List<string> NodeUrls { get; set; }
        public int MinimumNodes { get; set; }
        public int LatencyToleranceMs { get; set; } = 3000;
        public int ElectionTimeoutMs { get; set; } = 1000;
        public int DataTransferTimeoutMs { get; set; }
        public int MaxShardSize { get; set; }
    }
}
