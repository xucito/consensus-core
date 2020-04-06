using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Models
{
    [Serializable]
    public class LogEntry
    {
        public int Term { get; set; }
        //This will be null if it is the leader as the leader is responsible for ensuring the index positions match
        public int Index { get; set; }
        public IEnumerable<BaseCommand> Commands { get; set; }

        public LogEntry DeepCopy()
        {
            LogEntry other = (LogEntry)this.MemberwiseClone();
            return other;
        }
    }
}
