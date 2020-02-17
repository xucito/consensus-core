using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Enums
{
    public static class AppendEntriesExceptionNames
    {
        public static string ConflictingLogEntryException = "ConflictingLogEntryException";
        public static string MissingLogEntryException = "MissingLogEntryException";
        public static string OldTermException = "MissingLogEntryException";
        public static string LogAppendingException = "LogAppendingException";
    }
}
