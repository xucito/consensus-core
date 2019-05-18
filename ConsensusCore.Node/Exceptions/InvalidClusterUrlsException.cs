using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Exceptions
{
    public class InvalidClusterUrlsException : Exception
    {
        public InvalidClusterUrlsException()
        {
        }

        public InvalidClusterUrlsException(string message)
            : base(message)
        {
        }

        public InvalidClusterUrlsException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
