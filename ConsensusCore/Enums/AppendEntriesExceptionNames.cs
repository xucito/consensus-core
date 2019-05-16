﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Exceptions
{
    public static class AppendEntriesExceptionNames
    {
        public static string ConflictingLogEntryException = "ConflictingLogEntryException";
        public static string MissingLogEntryException = "MissingLogEntryException";
    }
}
