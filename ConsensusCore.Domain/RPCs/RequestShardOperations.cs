using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class RequestShardOperations : BaseRequest<RequestShardOperationsResponse>
    {
        public Guid ShardId { get; set; }
        public int From { get; set; }
        public int To { get; set; }
        public string Type { get; set; }

        public override string RequestName => "RequestShardOperation";
    }

    public class RequestShardOperationsResponse
    {
        public SortedDictionary<int, ShardOperationMessage> Operations { get; set; }
        public bool IsSuccessful { get; set; }
        public int LatestPosition { get; set; }
    }

    public class ShardOperationMessage
    {
        public ShardData Payload { get; set; }
        public int Position { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public Guid ObjectId { get; set; }
    }
}
