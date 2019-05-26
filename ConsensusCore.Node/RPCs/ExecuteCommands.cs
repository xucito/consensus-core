using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class ExecuteCommands : BaseRequest<bool>
    {
        public IEnumerable<BaseCommand> Commands { get; set; }
        public bool WaitForCommits { get; set; } = false;
        public override string RequestName { get => "RouteCommands"; }
    }
}
