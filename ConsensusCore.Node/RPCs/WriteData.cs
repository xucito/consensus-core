using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class WriteData : BaseRequest<WriteDataResponse>
    {
        public ShardData Data { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public bool WaitForSafeWrite { get; set; }
        public override string RequestName => "WriteData";
    }

    public class WriteDataResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
