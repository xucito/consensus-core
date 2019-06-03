using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
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
