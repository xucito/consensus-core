using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
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
        public int TimeoutMs = 3000;
        public bool CreateLock { get; set; }
        public int LockTimeoutMs { get; set; } = 30000;
    }

    public class RequestDataShardResponse: BaseResponse
    {
        public Guid? ShardId { get; set; }
        public Guid? NodeId { get; set; }
        public ShardData Data { get; set; }
        public string Type { get; set; }
        /// <summary>
        /// Whether a lock was successfully applied
        /// </summary>
        public bool AppliedLocked { get; set; } = false;
        public string SearchMessage { get; set; }
        public Guid? LockId { get; set; }
    }
}
