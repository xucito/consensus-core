using ConsensusCore.Node.Enums;
using ConsensusCore.Node.SystemCommands;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class ShardOperation
    {
        public ShardOperationOptions Operation { get; set; }
        public Guid ObjectId { get; set; }
    }
}
