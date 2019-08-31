using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    public class RemoveObjectLock : BaseCommand
    {
        public override string CommandName => "RemoveObjectLock";
        public Guid ObjectId { get; set; }
        public string Type { get; set; }
    }
}
