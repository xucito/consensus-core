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

        public Guid WriteData(string type, object data, Guid? objectId = null)
        {
            Guid? assignedGuid = objectId;
            if(assignedGuid == null)
            {
                assignedGuid = Guid.NewGuid();
            }

            switch (type)
            {
                case "number":
                    lock (locker)
                    {
                        if (_numberStore.ContainsKey(assignedGuid.Value))
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
            return assignedGuid.Value;
        }

        public object GetData(string type, Guid objectId)
        {
            return _numberStore[objectId];
        }
    }
}
