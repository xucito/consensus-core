using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.SystemCommands;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestShardUpdate : BaseRequest<RequestShardUpdateResponse>
    {
        public Guid ShardId { get; set; }
        public Guid ObjectId { get; set; }
        public UpdateShardAction Action { get; set; }
        public override string RequestName => "RequestShardUpdate";
    }

    public class RequestShardUpdateResponse
    {
        public bool IsSuccessful { get; set; } 
    }
    
}
