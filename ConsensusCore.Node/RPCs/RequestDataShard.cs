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
        /// <summary>
        /// Only specified if you know what shard to look into, otherwise it will do cascading primary node search
        /// </summary>
        public Guid? ShardId { get; set; }

        public override string RequestName => "GetDataShard";
    }

    public class RequestDataShardResponse
    {
        public bool IsSuccessful { get; set; }
        public object Data { get; set; }
    }
}
