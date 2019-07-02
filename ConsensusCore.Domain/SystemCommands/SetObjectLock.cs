using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    public class SetObjectLock : BaseCommand
    {
        public override string CommandName => "SetObjectLock";
        public Guid ObjectId { get; set; }
        public string Type { get; set; }
        public int TimeoutMs { get; set; }
    }
}
