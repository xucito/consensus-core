using ConsensusCore.Domain.BaseClasses;
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
        public HashSet<Guid> InsyncAllocationsToAdd { get; set; }
        public HashSet<Guid> InsyncAllocationsToRemove { get; set; }
        public HashSet<Guid> StaleAllocationsToAdd { get; set; }
        public HashSet<Guid> StaleAllocationsToRemove { get; set; }
        public int? LatestPos { get; set; }

        public override string CommandName => "UpdateShardMetadataAllocations";
    }
}
