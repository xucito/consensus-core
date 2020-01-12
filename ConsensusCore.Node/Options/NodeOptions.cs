using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node
{
    public class NodeOptions
    {
        // The node Id
        public Guid Id { get; set; }
        public string Name { get; set; }
        public bool EnableLeader { get; set; } = true;
        public bool AlwaysPrimary { get; set; } = false;
        public bool EnablePerformanceLogging { get; set; } = false;
        public bool PersistWriteQueue { get; set; } = false;
    }
}
