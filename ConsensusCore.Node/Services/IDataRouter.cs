using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public interface IDataRouter
    {
        void WriteData(ShardData data);
        object GetData(string type, Guid objectId);
    }
}
