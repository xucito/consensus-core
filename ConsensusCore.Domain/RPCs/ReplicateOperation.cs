using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class ReplicateShardOperation : BaseRequest<ReplicateShardOperationResponse>
    {
        public ShardOperation Operation { get; set; }
        public ShardData Payload { get; set; }
        public string Type { get; set; }

        public override string RequestName => "ReplicateShardOperation";
    }

    public class ReplicateShardOperationResponse: BaseResponse
    {
        public int LatestPosition { get; set; }
    }
}
