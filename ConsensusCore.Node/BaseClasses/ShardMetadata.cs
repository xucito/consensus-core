using System;
using System.Collections.Concurrent;
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
        /// <summary>
        /// Ids of what record is being stored here in this data shard and whether it has been confirmed to be written for the first time to a primary
        /// </summary>
        public ConcurrentDictionary<Guid, DataStates> DataTable { get; set; } 
        public int MaxSize { get; set; }
        public int ShardNumber { get; set; }
        public Guid Id { get; set; }
    }

    public enum DataStates
    {
        Assigned,
        Initialized,
        MarkedForDeletion
    }
}
