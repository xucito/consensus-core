﻿using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public interface IDataRouter
    {
        Guid WriteData(string type, object data, Guid? objectId = null);
        object GetData(string type, Guid shardId);
    }
}
