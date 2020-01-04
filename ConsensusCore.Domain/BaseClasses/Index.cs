using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class Index
    {
        public string Type { get; set; }
        public List<ShardAllocationMetadata> Shards { get; set; }
    }
}
