using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class ConflictingObjectLockException : Exception
    {

        public ConflictingObjectLockException()
        {
        }

        public ConflictingObjectLockException(string message)
            : base(message)
        {
        }

        public ConflictingObjectLockException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
