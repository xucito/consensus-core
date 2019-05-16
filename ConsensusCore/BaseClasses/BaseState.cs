using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.BaseClasses
{
    public abstract class BaseState<Command> where Command : BaseCommand
    {
        public BaseState() { }

        public abstract void ApplyCommand(Command command);
    }
}
