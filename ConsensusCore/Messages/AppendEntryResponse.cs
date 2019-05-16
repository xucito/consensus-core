using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Messages
{
    public class AppendEntryResponse
    {
        public bool Successful { get; set; }
        public bool HasConflictingEntry { get; set; }
    }
}
