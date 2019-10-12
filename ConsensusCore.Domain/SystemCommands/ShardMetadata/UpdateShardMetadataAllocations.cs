using ConsensusCore.Domain.BaseClasses;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands.ShardMetadata
{
    /// <summary>
    /// Used to update the allocations
    /// </summary>
    public class UpdateShardMetadataAllocations : BaseCommand
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public HashSet<Guid> InsyncAllocationsToAdd { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public HashSet<Guid> InsyncAllocationsToRemove { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public HashSet<Guid> StaleAllocationsToAdd { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public HashSet<Guid> StaleAllocationsToRemove { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public int? LatestPos { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public Guid? PrimaryAllocation { get; set; }

        public override string CommandName => "UpdateShardMetadataAllocations";
    }
}
