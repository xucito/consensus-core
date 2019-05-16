using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.BaseClasses
{
    public abstract class BaseCommand
    {
        public BaseCommand() { }

        public abstract bool IsEqual(LogEntry<BaseCommand> comparedEntry);
    }
}
