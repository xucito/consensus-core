using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class WriteData : BaseRequest<WriteDataResponse>
    {
        public ShardData Data { get; set; }
        public bool WaitForSafeWrite { get; set; }
        public override string RequestName => "WriteData";
    }

    public class WriteDataResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
