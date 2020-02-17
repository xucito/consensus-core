using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services.Tasks.Models
{
    public class AllocationCandidate
    {
        public Guid NodeId { get; set; }
        public string Type { get; set; }
    }
}
