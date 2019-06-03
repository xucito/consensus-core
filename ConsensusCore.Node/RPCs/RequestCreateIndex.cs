﻿using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestCreateIndex : BaseRequest<RequestCreateIndexResponse>
    {
        public string Type { get; set; }

        public override string RequestName => "RequestCreateIndex";
    }

    public class RequestCreateIndexResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
