using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs.Shard
{
    public class AddShardWriteOperation : BaseRequest<AddShardWriteOperationResponse>
    {
        public ShardData Data { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public bool WaitForSafeWrite { get; set; }
        public override string RequestName => "AddShardWriteOperation";
        public bool RemoveLock { get; set; } = false;//Remove any lock that may exist on the object
        public Guid LockId { get; set; }
        public bool Metric = true;
    }

    public class AddShardWriteOperationResponse : BaseResponse
    {
        /// <summary>
        /// Only appears if there is a certain level of consistenty
        /// </summary>
        public string OperationId { get; set; }
        public string ShardHash { get; set; }
        public int Pos { get; set; }
        public Guid ShardId { get; set; }
        public bool LockRemoved { get; set; } //Whether a lock was removed
        public List<Guid> FailedNodes { get; set; }
    }
}
