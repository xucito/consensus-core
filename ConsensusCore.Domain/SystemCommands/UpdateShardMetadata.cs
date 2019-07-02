using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    public class UpdateShardMetadata: BaseCommand
    {
        public Guid ShardId { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public string Type { get; set; }
        public List<Guid> InsyncAllocations { get; set; }
        public List<Guid> StaleAllocations { get; set; }

        public override string CommandName => "UpdateShardMetadata";
    }
}
