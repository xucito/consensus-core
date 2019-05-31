using ConsensusCore.Node.SystemCommands;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.ValueObjects
{
    public class ShardDataUpdate
    {
        public Guid DataId { get; set; }
        public UpdateShardAction Action { get; set; }
        public Guid ShardId { get; set; }
    }
}
