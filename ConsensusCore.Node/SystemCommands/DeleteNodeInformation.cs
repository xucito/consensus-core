using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class DeleteNodeInformation : BaseCommand
    {
        public override string CommandName => "DeleteNodeInformation";

        public Guid Id { get; set; }
    }
}
