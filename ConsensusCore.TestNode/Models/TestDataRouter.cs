using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.TestNode.Models
{
    public class TestDataRouter : IDataRouter
    {
        public Dictionary<Guid, TestData> _numberStore = new Dictionary<Guid, TestData>();
        object locker = new object();

        public object GetData(string type, Guid objectId)
        {
            if (_numberStore.ContainsKey(objectId))
                return _numberStore[objectId];
            return null;
        }

        public void WriteData(ShardData data)
        {
            switch (data)
            {
                case TestData t1:
                    _numberStore.Add(t1.Id, t1);
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
    }
}
