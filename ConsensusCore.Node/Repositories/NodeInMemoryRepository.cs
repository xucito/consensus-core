using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Services;
using System;
using System.Collections.Generic;

namespace ConsensusCore.Node.Repositories
{
    public class NodeInMemoryRepository : IBaseRepository
    {
        public NodeInMemoryRepository()
        {
        }

        public NodeStorage LoadNodeData()
        {
            return null;
        }

        public void SaveNodeData(NodeStorage storage)
        {
        }
    }
}
