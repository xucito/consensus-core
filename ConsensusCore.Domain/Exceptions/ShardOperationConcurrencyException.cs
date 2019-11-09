using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class ShardOperationConcurrencyException : Exception
    {

        public ShardOperationConcurrencyException()
        {
        }

        public ShardOperationConcurrencyException(string message)
            : base(message)
        {
        }

        public ShardOperationConcurrencyException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
