using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands.Tasks
{
    public class UpdateClusterTasks : BaseCommand
    {
        public override string CommandName => "UpdateClusterTasks";
        public List<BaseTask> TasksToAdd { get; set; }
        /// <summary>
        /// Pass the id of the tasks you want to remove
        /// </summary>
        public List<Guid> TasksToRemove { get; set; }
        public List<BaseTask> TasksToUpdate { get; set; }
    }
}
