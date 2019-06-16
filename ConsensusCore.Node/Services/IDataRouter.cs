using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public interface IDataRouter
    {
        void InsertData(ShardData data);
        void UpdateData(ShardData data);
        void DeleteData(ShardData data);
        ShardData GetData(string type, Guid objectId);
    }
}
