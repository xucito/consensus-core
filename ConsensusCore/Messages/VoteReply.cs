using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Messages
{
    public class VoteReply
    {
        public bool Success { get; set; }
        public Guid NodeId { get; set; }
    }
}
