using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class SharedShardMetadata
    {
        public Guid Id { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public List<Guid> InsyncAllocations { get; set; }
    }

    public enum DataStates
    {
        Assigned,
        Initialized,
        MarkedForDeletion
    }
}
