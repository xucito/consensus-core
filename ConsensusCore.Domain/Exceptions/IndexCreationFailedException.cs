using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class IndexCreationFailedException : Exception
    {
        public IndexCreationFailedException()
        {
        }

        public IndexCreationFailedException(string message)
            : base(message)
        {
        }

        public IndexCreationFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
