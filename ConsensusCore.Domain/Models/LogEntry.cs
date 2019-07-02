using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Models
{
    public class LogEntry
    {
        public int Term { get; set; }
        //This will be null if it is the leader as the leader is responsible for ensuring the index positions match
        public int Index { get; set; }
        public List<BaseCommand> Commands { get; set; }
    }
}
