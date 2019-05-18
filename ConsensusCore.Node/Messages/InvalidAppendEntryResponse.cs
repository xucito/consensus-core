using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Messages
{
    public class InvalidAppendEntryResponse
    {
        public string ConflictName { get; set; }
        public int? ConflictingTerm { get; set; }
        public int? FirstTermIndex { get; set; }
        public int? LastLogEntryIndex { get; set; }
    }
}
