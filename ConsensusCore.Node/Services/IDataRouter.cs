using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services
{
    public interface IDataRouter
    {
        Task<ShardData> InsertDataAsync(ShardData data);
        Task<ShardData> UpdateDataAsync(ShardData data);
        Task<bool> DeleteDataAsync(ShardData data);
        Task<ShardData> GetDataAsync(string type, Guid objectId);
    }
}
