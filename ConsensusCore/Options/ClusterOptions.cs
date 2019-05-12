using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Options
{
    public class ClusterOptions
    {
        public int KeepAliveIntervalMs { get; set; }
        public List<string> NodeUrls { get; set; }
        public int MinimumNodes { get; set; }
        public int LatencyToleranceMs { get; set; } = 3000;
        public int ElectionTimeoutMs { get; set; } = 1000;
    }
}
