using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    public class SetLock: BaseCommand
    {
        public override string CommandName => "SetLock";
        public string Name { get; set; }
        public Guid LockId { get; set; }
        public int TimeoutMs { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}
