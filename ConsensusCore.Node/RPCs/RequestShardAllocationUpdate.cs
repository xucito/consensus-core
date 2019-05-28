using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestShardAllocationUpdate : BaseRequest<RequestShardAllocationUpdateResponse>
    {
        public Guid ShardId { get; set; } 
        public Guid NodeId { get; set; }
        /// <summary>
        /// Set to -1 to delete the shard from the allocation
        /// </summary>
        public int Version { get; set; }

        public override string RequestName => "RequestShardAllocationUpdate";
    }

    public class RequestShardAllocationUpdateResponse
    {
        public bool IsSuccessful { get; set; } 
    }
}
