using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Models
{
    public class TestCommand : BaseCommand
    {
        public int ValueAdd;

        public override string CommandName => "TEST";
    }
}
