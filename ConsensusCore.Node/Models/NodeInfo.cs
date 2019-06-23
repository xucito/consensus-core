﻿using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Models
{
    public class NodeInfo
    {
        public Guid Id { get; set; }
        public bool InCluster { get; set; }
        public NodeStatus Status { get; set; }
        public int CommitIndex { get; set; }
        public Dictionary<Guid, int> ShardSyncPositions { get; set; }
        public Dictionary<Guid, int> ShardOperationCounts { get; set; }
    }

    public enum NodeStatus
    {
        Unknown,
        Red,
        Yellow,
        Green
    }
}
