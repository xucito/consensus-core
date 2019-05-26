using ConsensusCore.Node;
using ConsensusCore.Node.Repositories;
using ConsensusCore.TestNode.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConcensusCore.Node.Tests.SingleNode
{
    public class SingleNodeFixture : IDisposable
    {
        public ConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>> Node;

        public SingleNodeFixture()
        {
            var moqClusterOptions = new Mock<IOptions<ClusterOptions>>();
            moqClusterOptions.Setup(mqo => mqo.Value).Returns(new ClusterOptions() { });

            var moqNodeOptions = new Mock<IOptions<NodeOptions>>();
            moqNodeOptions.Setup(mqo => mqo.Value).Returns(new NodeOptions() { });

            var serviceProvider = new ServiceCollection()
            .AddLogging()
            .BuildServiceProvider();

            var factory = serviceProvider.GetService<ILoggerFactory>();

            var logger = factory.CreateLogger<ConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>>>();

            var inMemoryRepository = new NodeInMemoryRepository<TestCommand>();
            Node = new ConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>>(moqClusterOptions.Object,
            moqNodeOptions.Object,
            new ConsensusCore.Node.Services.NodeStorage<TestCommand, NodeInMemoryRepository<TestCommand>>(inMemoryRepository)
            {
                CurrentTerm = 5
            },
            logger, new ConsensusCore.Node.Interfaces.StateMachine<TestCommand, TestState>());

        }

        public void Dispose()
        {
        }
    }
}
