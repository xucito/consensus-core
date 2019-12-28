using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class ShardWriteOperationConcurrencyException : Exception
    {

        public ShardWriteOperationConcurrencyException()
        {
        }

        public ShardWriteOperationConcurrencyException(string message)
            : base(message)
        {
        }

        public ShardWriteOperationConcurrencyException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
