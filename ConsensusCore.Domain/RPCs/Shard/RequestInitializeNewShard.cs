using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs.Shard
{
    public class AllocateShard : BaseRequest<AllocateShardResponse>
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }

        public override string RequestName => "AllocateShard";
    }

    public class AllocateShardResponse : BaseResponse
    {
    }
}
