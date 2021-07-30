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
        /// <summary>
        /// The class to deserialize the data as
        /// </summary>
        public string ClassName { get { return this.GetType().FullName; } }
        public Guid PrimaryAllocation { get; set; }
        public HashSet<Guid> InsyncAllocations { get; set; } = new HashSet<Guid>();
        public HashSet<Guid> StaleAllocations { get; set; } = new HashSet<Guid>();
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
