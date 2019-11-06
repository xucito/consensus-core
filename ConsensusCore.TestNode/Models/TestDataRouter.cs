using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Node.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Models
{
    public class TestDataRouter : IDataRouter
    {
        public ConcurrentDictionary<Guid, TestData> _numberStore = new ConcurrentDictionary<Guid, TestData>();
        object locker = new object();

        public async Task<bool> DeleteDataAsync(ShardData data)
        {
            _numberStore.TryRemove(data.Id, out _);
            return true;
        }

        public async Task<ShardData> GetDataAsync(string type, Guid objectId)
        {
            if (_numberStore.ContainsKey(objectId))
                return _numberStore[objectId];
            return null;
        }

        public async Task<ShardData> InsertDataAsync(ShardData data)
        {
            switch (data)
            {
                case TestData t1:
                    var addResult = _numberStore.TryAdd(t1.Id, t1);
                    if (!addResult)
                    {
                        Console.WriteLine("Failed to insert data, there seems to be a concurrency issue! The data must already exist for object..." + t1.Id + " replacing the existing data" + Environment.NewLine + JsonConvert.SerializeObject(data, Formatting.Indented));
                        await UpdateDataAsync(data);
                    }
                    break;
            }
            return data;

            /*
            switch (data.Type)
            {
                case "number":
                    lock (locker)
                    {
                        if (_numberStore.ContainsKey(data.Value))
                        {
                            _numberStore[assignedGuid.Value] = Convert.ToInt32(data);
                        }
                        else
                        {
                            _numberStore.Add(assignedGuid.Value, Convert.ToInt32(data));
                        }
                    }
                    return assignedGuid.Value;
            }
            return assignedGuid.Value;*/
        }

        public async Task<ShardData> UpdateDataAsync(ShardData data)
        {
            switch (data)
            {
                case TestData t1:
                    _numberStore[data.Id] = t1;
                    /*if (!updateResult)
                    {
                        throw new Exception("Failed to update data " + data.Id + " on shard " + data.ShardId);
                    }*/
                    break;
                default:
                    throw new Exception("Failed to match the data with a type");
            }
            return data;
        }
    }
}
