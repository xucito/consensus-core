using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class WriteData : BaseRequest<WriteDataResponse>
    {
        public ShardData Data { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public bool WaitForSafeWrite { get; set; }
        public override string RequestName => "WriteData";
    }

    public class WriteDataResponse: BaseResponse
    {
        public Guid ShardId { get; set; }
    }
}
