using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Services;
using System;
using System.Collections.Generic;

namespace ConsensusCore.Node.Repositories
{
    public class NodeInMemoryRepository<Z> : IBaseRepository<Z>
        where Z : BaseState, new()
    {
        public NodeInMemoryRepository()
        {
        }

        public NodeStorage<Z> LoadNodeData()
        {
            return new NodeStorage<Z>()
            {
                Id = Guid.NewGuid()
            };
        }

        public void SaveNodeData(NodeStorage<Z> storage)
        {
        }
    }
}
