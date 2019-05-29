using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class CreateDataShardInformation: BaseCommand
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        public Guid[] InsyncAllocations { get; set; }
        public Guid PrimaryAllocation { get; set; }
        public int Version { get; set; }
        public bool Initalized { get; set; }
        public Dictionary<Guid, int> Allocations { get; set; }
        public int ShardNumber { get; set; }
        public int MaxSize { get; set; }

        public override string CommandName => "CreateDataShardInformation";
    }
}
