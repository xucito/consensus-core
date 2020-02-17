using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Utility
{
    public class PerformanceMetricUtility
    {
        public static void PrintCheckPoint(ref object lockObject, ref ConcurrentDictionary<int, double> totals, ref DateTime startTime, ref int checkpointNumber, string name)
        {
            lock (lockObject)
            {
                totals.AddOrUpdate(checkpointNumber, (DateTime.Now - startTime).TotalMilliseconds, (a, b) => { return a + b; });
            }
            startTime = DateTime.Now;
            checkpointNumber++;
        }
    }
}
