using ConsensusCore.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Samples.Calculator
{
    public class Calculate : BaseCommand
    {
        public char Operator { get; set; }
        public double Number { get; set; }

        public override bool IsEqual(LogEntry<BaseCommand> comparedEntry)
        {
            throw new NotImplementedException();
        }
    }
}
