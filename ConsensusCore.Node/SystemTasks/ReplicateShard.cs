using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemTasks
{
    public class ReplicateShard: BaseTask
    {
        public Guid ShardId { get; set; }

        public override string Name => "ReplicateShard";
    }
}
