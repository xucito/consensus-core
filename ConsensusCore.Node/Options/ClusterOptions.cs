using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node
{
    public class ClusterOptions
    {
        public string NodeUrls { get; set; }
        public int MinimumNodes { get; set; }
        public int LatencyToleranceMs { get; set; } = 3000;
        public int ElectionTimeoutMs { get; set; } = 1000;
        public int DataTransferTimeoutMs { get; set; } = 30000;
        public int NumberOfShards { get; set; } = 1;
        public int ConcurrentTasks { get; set; } = 4;
        public bool TestMode { get; set; }
        public int CommitsTimeout { get; set; } = 10000;
        public int MaxLogsToSend { get; set; } = 100;
        public int MaxObjectSync { get; set; } = 100;
        public int SnapshottingInterval { get; set; } = 50;
        public int SnapshottingTrailingLogCount { get; set; } = 10;
        /// <summary>
        /// How many positions to validate when recovering a shard
        /// </summary>
        public int ShardRecoveryValidationCount { get; set; } = 50;
        public bool DebugMode { get; set; } = false;
    }
}
