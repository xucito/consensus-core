using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class UpsertNodeInformation : BaseCommand
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string TransportAddress { get; set; }
        public bool IsContactable { get; set; }

        public override string CommandName => "UpsertNodeInformation";
    }
}
