using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Repositories
{
    public partial class NodeInMemoryRepository<Z> : IBaseRepository<Z>
        where Z : BaseState, new()
    {
        public Task<NodeStorage<Z>> LoadNodeDataAsync()
        {
            return Task.FromResult(new NodeStorage<Z>()
            {
                Id = Guid.NewGuid()
            });
        }

        public Task<bool> SaveNodeDataAsync(NodeStorage<Z> storage)
        {
            return Task.FromResult(true);
        }
    }
}
