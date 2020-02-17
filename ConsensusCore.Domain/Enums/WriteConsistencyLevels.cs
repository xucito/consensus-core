using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Enums
{
    public enum WriteConsistencyLevels
    {
        // Ensure transaction is queued on the local node
        Queued,
        // Ensure transaction is written to the primary
        Primary,
        // Ensure the transaction is replicated to at least one other node
        Replicated,
        // Ensure the transaction is replicated to a majority of other allocations
        Majority
    }
}
