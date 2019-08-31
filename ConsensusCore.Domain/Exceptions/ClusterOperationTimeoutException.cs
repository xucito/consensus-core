using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class ClusterOperationTimeoutException : Exception
    {
        public ClusterOperationTimeoutException()
        {
        }

        public ClusterOperationTimeoutException(string message)
            : base(message)
        {
        }

        public ClusterOperationTimeoutException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
