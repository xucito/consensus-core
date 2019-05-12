using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.ViewModels
{
    public class NodeInfo
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public double Version { get; set; }
        public int CurrentTerm { get; set; } = 0;
        public Guid? VotedFor { get; set; } = null;
        public Dictionary<int, string> Logs { get; set; } = new Dictionary<int, string>();
    }
}
