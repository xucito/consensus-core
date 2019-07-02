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
        public int DataTransferTimeoutMs { get; set; } = 30000;
        public int NumberOfShards { get; set; }
        public int ConcurrentTasks { get; set; }
        public bool TestMode { get; set; }
        public int CommitsTimeout { get; set; } = 10000;
        public int MaxLogsToSend { get; set; } = 100;
    }
}
