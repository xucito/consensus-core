using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Connectors.Exceptions
{
    public class MissingNodeConnectionDetailsException : Exception
    {
        public MissingNodeConnectionDetailsException()
        {
        }

        public MissingNodeConnectionDetailsException(string message)
            : base(message)
        {
        }

        public MissingNodeConnectionDetailsException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
