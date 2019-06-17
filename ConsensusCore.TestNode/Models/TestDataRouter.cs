using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Services;
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

        public void DeleteData(ShardData data)
        {
            _numberStore.TryRemove(data.Id, out _);
        }

        public ShardData GetData(string type, Guid objectId)
        {
            if (_numberStore.ContainsKey(objectId))
                return _numberStore[objectId];
            return null;
        }

        public void InsertData(ShardData data)
        {
            switch (data)
            {
                case TestData t1:
                    _numberStore.TryAdd(t1.Id, t1);
                    break;
            }

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

        public void UpdateData(ShardData data)
        {
            switch (data)
            {
                case TestData t1:
                    if (_numberStore.ContainsKey(data.Id))
                    {
                        _numberStore[data.Id] = t1;
                    }
                    break;
            }
        }
    }
}
