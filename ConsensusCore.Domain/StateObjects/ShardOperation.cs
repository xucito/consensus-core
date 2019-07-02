using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class ShardOperation
    {
        public ShardOperationOptions Operation { get; set; }
        public Guid ObjectId { get; set; }
        public bool Applied { get; set; } = false;
    }
}
