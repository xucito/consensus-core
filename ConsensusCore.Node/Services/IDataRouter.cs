using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public interface IDataRouter
    {
        bool SaveData(string type, Guid shardId, object data);
        object GetData(string type, Guid shardId);
    }
}
