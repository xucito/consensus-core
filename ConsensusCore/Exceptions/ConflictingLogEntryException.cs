using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Exceptions
{
    public class ConflictingLogEntryException : Exception
    {
        public int ConflictingTerm { get; set; }
        public int FirstTermIndex { get; set; }

        public ConflictingLogEntryException()
        {
        }

        public ConflictingLogEntryException(string message)
            : base(message)
        {
        }

        public ConflictingLogEntryException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
