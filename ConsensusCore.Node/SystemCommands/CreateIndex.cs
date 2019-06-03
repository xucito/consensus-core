using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class CreateIndex: BaseCommand
    {
        public string Type { get; set; }
        public List<SharedShardMetadata> Shards { get; set; }

        public override string CommandName => "CreateIndex";
    }
}
