using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.SystemCommands;
using ConsensusCore.Node.ValueObjects;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestShardUpdate : BaseRequest<RequestShardUpdateResponse>
    {
        public Dictionary<Guid, ShardDataUpdate> Updates { get; set; }
        public override string RequestName => "RequestShardUpdate";
    }

    public class RequestShardUpdateResponse
    {
        public bool IsSuccessful { get; set; } 
    }
    
}
