using ConsensusCore.Node.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Models
{
    public class TestDataRouter : IDataRouter
    {
        Dictionary<Guid, int> _numberStore = new Dictionary<Guid, int>();
        object locker = new object();

        public bool SaveData(string type, Guid shardId, object data)
        {
            switch (type)
            {
                case "number":
                    lock (locker)
                    {
                        if (_numberStore.ContainsKey(shardId))
                        {
                            _numberStore[shardId] = (int)data;
                        }
                        else
                        {
                            _numberStore.Add(shardId, (int)data);
                        }
                    }
                    return true;
            }
            return false;
        }

        public object GetData(string type, Guid shardId)
        {
            return _numberStore[shardId];
        }
    }
}
