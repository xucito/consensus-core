using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs.Shard
{ 
    public class RequestShardWriteOperations : BaseRequest<RequestShardWriteOperationsResponse>
    {
        public Guid ShardId { get; set; }
        public int From { get; set; }
        public int To { get; set; }
        public string Type { get; set; }

        public override string RequestName => "RequestShardWriteOperations";
    }

    public class RequestShardWriteOperationsResponse: BaseResponse
    {
        public SortedDictionary<int, ShardWriteOperation> Operations { get; set; }
        public bool IsSuccessful { get; set; }
        public int LatestPosition { get; set; }
    }
}
