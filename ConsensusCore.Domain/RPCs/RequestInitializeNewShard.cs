using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class RequestInitializeNewShard : BaseRequest<RequestInitializeNewShardResponse>
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }

        public override string RequestName => "RequestInitializeNewShard";
    }

    public class RequestInitializeNewShardResponse
    {
    }
}
