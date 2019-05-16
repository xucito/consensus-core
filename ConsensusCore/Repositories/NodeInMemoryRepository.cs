using ConsensusCore.BaseClasses;
using ConsensusCore.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Repositories
{
    public class NodeInMemoryRepository<Command> : INodeRepository<Command>
        where Command: BaseCommand
    {
        public NodeInfo<Command> NodeInfo {get;set;}

        public NodeInMemoryRepository()
        {
            NodeInfo = new NodeInfo<Command>(new List<LogEntry<Command>>())
            {
                Id = Guid.NewGuid(),
                Name = "Node 1",
                Version = 1.0,
                VotedFor = null,
                CurrentTerm = 0
            };
        }

        public NodeInfo<Command> LoadConfiguration()
        {
            return NodeInfo;
        }
    }
}
