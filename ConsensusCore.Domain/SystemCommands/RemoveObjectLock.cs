using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.SystemCommands
{
    /// <summary>
    /// Remove a object based on lockId or objectId and type
    /// If lockId is specified, it will search for the lock only using lockId, if no LockId is given, it will check ObjectId and Type (Conflicting ObjectId/Type and LockId will resolve using LockId)
    /// </summary>
    public class RemoveObjectLock : BaseCommand
    {
        public override string CommandName => "RemoveObjectLock";
        public Guid ObjectId { get; set; }
        public string Type { get; set; }
        public Guid? LockId { get; set; }
    }
}
