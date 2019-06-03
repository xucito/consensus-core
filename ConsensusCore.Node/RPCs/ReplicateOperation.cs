using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.SystemCommands;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class ReplicateShardOperation : BaseRequest<ReplicateShardOperationResponse>
    {
        public Guid ShardId { get; set; }
        public ShardOperation Operation { get; set; }
        public ShardData Payload { get; set; }
        public int Pos { get; set; }

        public override string RequestName => "ReplicateShardOperation";
    }

    public class ReplicateShardOperationResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
