using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Exceptions
{
    public class MissingLogEntryException : Exception
    {
        public int LastLogEntryIndex { get; set; }

        public MissingLogEntryException()
        {
        }

        public MissingLogEntryException(string message)
            : base(message)
        {
        }

        public MissingLogEntryException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
