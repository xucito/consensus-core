﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class SharedShardMetadata
    {
        public Guid Id { get; set; }
        public string Type { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public List<Guid> InsyncAllocations { get; set; } = new List<Guid>();
        public List<Guid> StaleAllocations { get; set; } = new List<Guid>();
    }

    public enum DataStates
    {
        Assigned,
        Initialized,
        MarkedForDeletion
    }
}