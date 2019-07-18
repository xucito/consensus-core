using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemTasks
{
    public class RecoverShard: BaseTask
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }

        public override string Name => "ResyncShard";
    }
}
