using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class UpdateShardAllocation
    {
        public Dictionary<Guid, int> Allocations { get; set; }
        public Guid ShardId { get; set; }
    }
}
