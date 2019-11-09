using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Models
{
    public class TestState : BaseState
    {
        public List<int> Values = new List<int>();

        public override void ApplyCommandToState(BaseCommand command)
        {
            switch(command)
            {
                case TestCommand t1:
                    TestCommand testCommand = (TestCommand)command;
                    Values.Add(testCommand.ValueAdd);
                    break;
            }
        }
    }
}
