using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class SharedShardMetadata
    {
        public Guid Id { get; set; }
        public string Type { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public HashSet<Guid> InsyncAllocations { get; set; } = new HashSet<Guid>();
        public HashSet<Guid> StaleAllocations { get; set; } = new HashSet<Guid>();
    }

    public enum DataStates
    {
        Assigned,
        Initialized,
        MarkedForDeletion
    }
}
