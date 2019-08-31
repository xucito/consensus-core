using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    public class NodeInfo
    {
        public Guid Id { get; set; }
        public bool InCluster { get; set; }
        public NodeStatus Status { get; set; }
        public int CommitIndex { get; set; }
        public Dictionary<Guid, int> ShardSyncPositions { get; set; }
        public Dictionary<Guid, int> ShardOperationCounts { get; set; }
        public object ThreadCounts { get; set; }
        public string CurrentRole { get; set; }
        public int Term { get; set; }
        public int LatestLeaderCommit { get; set; }
    }

    public enum NodeStatus
    {
        Unknown,
        Red,
        Yellow,
        Green
    }
}
