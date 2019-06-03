using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class UpsertClusterTasks: BaseCommand
    {
         public List<BaseTask> ClusterTasks { get; set; }

        public override string CommandName => "UpsertClusterTasks";
    }
}
