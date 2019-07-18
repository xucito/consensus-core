using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    /// <summary>
    /// This is only managed by the leader.
    /// </summary>
    public class UpdateShardMetadata: BaseCommand
    {
        public Guid ShardId { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public string Type { get; set; }
        public HashSet<Guid> InsyncAllocations { get; set; }
        public HashSet<Guid> StaleAllocations { get; set; }

        public override string CommandName => "UpdateShardMetadata";
    }
}
