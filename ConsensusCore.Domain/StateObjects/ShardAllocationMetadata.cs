using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class ShardAllocationMetadata
    {
        public Guid Id { get; set; }
        public string Type { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public HashSet<Guid> InsyncAllocations { get; set; } = new HashSet<Guid>();
        public HashSet<Guid> StaleAllocations { get; set; } = new HashSet<Guid>();
        /// <summary>
        /// The latest operation detected by the leader
        /// </summary>
        public int LatestOperationPos { get; set; }

        public ShardAllocationMetadata DeepCopy()
        {
            ShardAllocationMetadata other = (ShardAllocationMetadata)this.MemberwiseClone();
            return other;
        }
    }

    public enum DataStates
    {
        Assigned,
        Initialized,
        MarkedForDeletion
    }
}
