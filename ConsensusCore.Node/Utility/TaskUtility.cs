using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Utility
{
    public static class TaskUtility
    {
        public static void RestartTask(ref Task task, Func<Task> threadFunction)
        {
            if (task == null || task.IsCompleted)
            {
                task = Task.Run(() => threadFunction());
            }
        }
    }
}
