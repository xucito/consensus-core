using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class BaseResponse
    {
        public BaseResponse() { }

        public bool IsSuccessful { get; set; }
    }
}
