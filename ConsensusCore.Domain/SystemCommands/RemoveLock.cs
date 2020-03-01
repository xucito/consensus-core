using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    public class RemoveLock: BaseCommand
    {
        public override string CommandName => "RemoveLock";
        public string Name { get; set; }
        public Guid LockId { get; set; }
    }
}
