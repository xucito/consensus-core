using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    public class DataReversionRecord
    {
        public ShardOperation OriginalOperation { get; set; }
        public ShardData OriginalData { get; set; }
        public ShardData NewData { get; set; }
        public string DebugMessage { get; set; }
        public DateTime RevertedTime { get; set; }
    }
}
