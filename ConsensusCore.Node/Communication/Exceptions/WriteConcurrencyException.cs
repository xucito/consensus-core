using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Communication.Exceptions
{
    public class WriteConcurrencyException : Exception
    {
        public WriteConcurrencyException()
        {
        }

        public WriteConcurrencyException(string message)
            : base(message)
        {
        }

        public WriteConcurrencyException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
