using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class AssignDataToShard : BaseRequest<AssignDataToShardResponse>
    {
        public override string RequestName => "AssignDataToShard";
        public string Type { get; set; }
        public Guid ObjectId { get; set; }
    }

    public class AssignDataToShardResponse
    {
        public bool IsSuccessful { get; set; }
        public Guid AssignedShard { get; set; }
    }
}