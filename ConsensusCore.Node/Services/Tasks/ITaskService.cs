using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;

namespace ConsensusCore.Node.Services.Tasks
{
    public interface ITaskService
    {
        Task StartNodeTask(BaseTask task);
        List<Guid> RunningTasks { get; }
    }
}