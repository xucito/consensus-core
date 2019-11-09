using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Enums
{
    public enum NodeState {
        Leader,
        Follower,
        Candidate,
        Disabled
    }
}
