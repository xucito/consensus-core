using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services
{
    public interface IDataRouter
    {
        /// <summary>
        /// When you insert data, safely ignore the insert if the data already exists.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        Task<ShardData> InsertDataAsync(ShardData data);
        Task<ShardData> UpdateDataAsync(ShardData data);
        Task<bool> DeleteDataAsync(ShardData data);
        Task<ShardData> GetDataAsync(string type, Guid objectId);
    }
}
