using ConsensusCore.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Repositories
{
    public class NodeInMemoryRepository : INodeRepository
    {
        public NodeInfo NodeInfo {get;set;}

        public NodeInMemoryRepository()
        {
            NodeInfo = new NodeInfo()
            {
                Id = Guid.NewGuid(),
                Name = "Node 1",
                Version = 1.0,
                VotedFor = null,
                CurrentTerm = 0
            };
        }

        public NodeInfo LoadConfiguration()
        {
            return NodeInfo;
        }
    }
}
