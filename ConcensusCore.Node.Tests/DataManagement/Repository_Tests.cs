using ConsensusCore.Node.Repositories;
using ConsensusCore.TestNode.Models;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ConcensusCore.Node.Tests.DataManagement
{
    public class Repository_Tests
    {
        NodeInMemoryRepository<TestState> _repository;

        public Repository_Tests()
        {
            _repository = new NodeInMemoryRepository<TestState>();
        }

        [Fact]
        public async void GetNullShardWriteOperation()
        {
            var result = await _repository.GetShardWriteOperationAsync(Guid.NewGuid(), 1);
        }

    }
}
