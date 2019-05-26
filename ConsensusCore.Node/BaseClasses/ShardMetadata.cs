﻿using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class ShardMetadata
    {
        public string Type { get; set; }
        public Dictionary<Guid, int> Allocations { get; set; }
       // public Guid[] InsyncAllocations { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public int Version { get; set; }
        // Whether the shard has been successfully created with first version of data
        public bool Initalized { get; set; }
    }
}