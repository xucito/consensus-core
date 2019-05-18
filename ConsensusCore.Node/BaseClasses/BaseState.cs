using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.BaseClasses
{
    public abstract class BaseState<TCommand> where TCommand : BaseCommand
    {
        public BaseState() { }

        public abstract void ApplyCommand(TCommand command);
    }
}
