using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class DefaultCommand : BaseCommand
    {
        public override string CommandName => "DefaultCommand";
    }
}
