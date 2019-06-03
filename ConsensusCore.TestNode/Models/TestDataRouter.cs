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
        public List<TestData> _numberStore = new List<TestData>();
        object locker = new object();

        public object GetData(string type, Guid objectId)
        {
            return null;
            //   return _numberStore[objectId];
        }

        public void WriteData(ShardData data)
        {
            switch (data)
            {
                case TestData t1:
                    _numberStore.Add(t1);
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
