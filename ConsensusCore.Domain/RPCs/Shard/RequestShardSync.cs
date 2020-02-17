using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs.Shard
{
    public class RequestShardSync : BaseRequest<RequestShardSyncResponse>
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }

        public override string RequestName => "RequestShardSync";
    }

    public class RequestShardSyncResponse : BaseResponse
    {
    }
}
