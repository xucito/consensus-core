using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Models
{
    public class TestState : BaseState<TestCommand>
    {
        public override void ApplyCommand(TestCommand command)
        {
            throw new NotImplementedException();
        }
    }
}
