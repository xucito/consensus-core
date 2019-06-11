using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestShardOperations : BaseRequest<RequestShardOperationsResponse>
    {
        public int OperationPos { get; set; }
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        public override string RequestName => "RequestShardOperations";
    }

    public class RequestShardOperationsResponse
    {
        public bool IsSuccessful { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public Guid ObjectId { get; set; }
        public ShardData Payload { get; set; }
        public int LatestOperationNo { get; set; }
        public int Pos { get; set; }
    }
}
