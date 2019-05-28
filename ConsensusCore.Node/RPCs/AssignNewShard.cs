using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class AssignNewShard : BaseRequest<AssignNewShardResponse>
    {
        public override string RequestName => "AssignNewShard";
        public Guid ShardId { get; set; }
        public string Type { get; set; }
    }

    public class AssignNewShardResponse
    {
        public bool IsSuccessful { get; set; }
    }
}