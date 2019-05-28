using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class UpdateShardAllocation : BaseCommand
    {
        public Guid ShardId { get; set; }
        public Guid NodeId { get; set; }
        /// <summary>
        /// Set to -1 to delete the shard from the allocation
        /// </summary>
        public int Version { get; set; }

        public override string CommandName => "UpdateShardAllocation";
    }
}
