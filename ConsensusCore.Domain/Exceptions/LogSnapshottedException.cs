using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class LogSnapshottedException : Exception
    {
        public LogSnapshottedException()
        {
        }

        public LogSnapshottedException(string message)
            : base(message)
        {
        }

        public LogSnapshottedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
