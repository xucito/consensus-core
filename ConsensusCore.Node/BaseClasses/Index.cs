using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class Index
    {
        public string Type { get; set; }
        public List<SharedShardMetadata> Shards { get; set; }
    }
}
