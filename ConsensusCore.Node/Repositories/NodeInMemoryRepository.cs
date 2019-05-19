using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Repositories
{
    public class NodeInMemoryRepository<TCommand> : BaseRepository<TCommand>
        where TCommand : BaseCommand
    {
        public NodeInMemoryRepository()
        {

        }

        public override NodeStorage<TCommand> LoadNodeData()
        {
            return new NodeStorage<TCommand>()
            {
                Id = Guid.NewGuid(),
                CurrentTerm = 0,
                Logs = new List<LogEntry<TCommand>>(),
                Name = "",
                Version = 1.0
            };
        }

        public override void SaveNodeData()
        {

        }
    }
}
