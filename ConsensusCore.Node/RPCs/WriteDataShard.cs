using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class WriteDataShard : BaseRequest<WriteDataShardResponse>
    {
        public string Type { get; set; }
        public Guid? ShardId { get; set; }
        public object Data { get; set; }
        public int? Version { get; set; }
        // Wait for at least two copies of the data to be present
        public bool WaitForSafeWrite { get; set; }
        public override string RequestName => "WriteDataShard";
    }

    public class WriteDataShardResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
