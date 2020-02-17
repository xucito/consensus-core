using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services.Data.Components.ValueObjects
{
    public class WriteShardDataResponse
    {
        public int Pos { get; set; }
        public string ShardHash { get; set; }
        public bool IsSuccessful { get; set; }
    }
}
