using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class ReplicateShardOperation : BaseRequest<ReplicateShardOperationResponse>
    {
        public Guid ShardId { get; set; }
        public ShardOperation Operation { get; set; }
        public ShardData Payload { get; set; }
        public int Pos { get; set; }
        public string Type { get; set; }

        public override string RequestName => "ReplicateShardOperation";
    }

    public class ReplicateShardOperationResponse
    {
        public int LatestPosition { get; set; }
        public bool IsSuccessful { get; set; }
    }
}
