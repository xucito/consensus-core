using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class ExecuteCommands : BaseRequest<ExecuteCommandsResponse>
    {
        public IEnumerable<BaseCommand> Commands { get; set; }
        public bool WaitForCommits { get; set; } = false;
        public override string RequestName { get => "RouteCommands"; }
    }

    public class ExecuteCommandsResponse : BaseResponse
    {
        public int EntryNo { get; set; }
    }
}
