using ConsensusCore.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Samples.Calculator
{
    public class CalculatorState: BaseState<Calculate>
    {
        public double? StoredValue { get; set; } = 0;

        public CalculatorState()
        { }

        public override void ApplyCommand(Calculate command)
        {
            switch (command.Operator)
            {
                case '-':
                    StoredValue -= command.Number;
                    break;
                case '+':
                    StoredValue += command.Number;
                    break;
                case '/':
                    StoredValue /= command.Number;
                    break;
                case '*':
                    StoredValue *= command.Number;
                    break;
                default:
                    StoredValue = null;
                    break;
            }
        }
    }
}
