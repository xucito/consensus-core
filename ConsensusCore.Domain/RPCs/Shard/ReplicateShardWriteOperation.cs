using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs.Shard
{
    public class ReplicateShardWriteOperation : BaseRequest<ReplicateShardWriteOperationResponse>
    {
        public ShardWriteOperation Operation { get; set; }

        public override string RequestName => "ReplicateShardOperation";
    }

    public class ReplicateShardWriteOperationResponse : BaseResponse
    {
        public int LatestPosition { get; set; }
    }
}
