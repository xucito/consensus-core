using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Messages
{
    public class RequestVote
    {
        public int Term { get; set; }
        public Guid CandidateId {get;set;}
        public int LastLogIndex { get; set; }
        public int LastLogTerm { get; set; }
        public Guid NodeId { get; set; }
    }
}
