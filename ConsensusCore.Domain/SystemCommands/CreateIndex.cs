using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    public class CreateIndex: BaseCommand
    {
        public string Type { get; set; }
        public List<ShardAllocationMetadata> Shards { get; set; }

        public override string CommandName => "CreateIndex";
    }
}
