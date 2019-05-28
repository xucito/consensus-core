using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestDataShard : BaseRequest<RequestDataShardResponse>
    {
        public Guid ObjectId { get; set; }
        public string Type { get; set; }

        public override string RequestName => "GetDataShard";
    }

    public class RequestDataShardResponse
    {
        public object Data { get; set; }
    }
}
