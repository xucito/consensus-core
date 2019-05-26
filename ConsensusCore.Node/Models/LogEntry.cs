using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Models
{
    public class LogEntry
    {
        public int Term { get; set; }
        public int Index { get; set; }
        public List<BaseCommand> Commands { get; set; }
    }
}
