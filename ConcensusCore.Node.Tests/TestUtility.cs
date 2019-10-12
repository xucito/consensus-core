using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.TestNode.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConcensusCore.Node.Tests
{
    public static class TestUtility
    {
        public static Guid DefaultShardId = Guid.NewGuid();

        public static ConsensusCoreNode<TestState, IBaseRepository<TestState>> GetTestConsensusCoreNode()
        {
            var moqClusterOptions = new Mock<IOptions<ClusterOptions>>();
            moqClusterOptions.Setup(mqo => mqo.Value).Returns(new ClusterOptions()
            {
                NodeUrls = "localhost:5022",
                TestMode = true,
                NumberOfShards = 1,
                DataTransferTimeoutMs = 1000,
                ElectionTimeoutMs = 1000,
                LatencyToleranceMs = 1000,
                MinimumNodes = 1
            });

            var moqNodeOptions = new Mock<IOptions<NodeOptions>>();
            moqNodeOptions.Setup(mqo => mqo.Value).Returns(new NodeOptions() { });

            var serviceProvider = new ServiceCollection()
            .AddLogging()
            .BuildServiceProvider();

            var factory = serviceProvider.GetService<ILoggerFactory>();

            var logger = factory.CreateLogger<ConsensusCoreNode<TestState, IBaseRepository<TestState>>>();

            IBaseRepository<TestState> inMemoryRepository = (IBaseRepository<TestState>)new NodeInMemoryRepository<TestState>();
            var NodeStorage = new NodeStorage<TestState>(inMemoryRepository) { };
            var _dataRouter = new TestDataRouter();
            var _stateMachine = new StateMachine<TestState>();
            var _connector = new ClusterConnector(TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(1000));

            return new ConsensusCoreNode<TestState, IBaseRepository<TestState>>(moqClusterOptions.Object,
            moqNodeOptions.Object,
            logger,
            _stateMachine,
            inMemoryRepository,
           _connector,
            _dataRouter,
            new ShardManager<TestState, IBaseRepository<TestState>>(_stateMachine,
                inMemoryRepository,
                factory.CreateLogger<ShardManager<TestState, IBaseRepository<TestState>>>(),
            _connector,
            _dataRouter,
            moqClusterOptions.Object,
            NodeStorage),
            NodeStorage
            );
        }

        public static ShardManager<TestState, IBaseRepository<TestState>> GetTestShardManager()
        {
            var serviceProvider = new ServiceCollection()
            .AddLogging()
            .BuildServiceProvider();
            var _stateMachine = new StateMachine<TestState>()
            {

            };
            var factory = serviceProvider.GetService<ILoggerFactory>();
            IBaseRepository<TestState> inMemoryRepository = (IBaseRepository<TestState>)new NodeInMemoryRepository<TestState>();
            var _connector = new ClusterConnector(TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(1000));
            var _dataRouter = new TestDataRouter();
            var moqClusterOptions = new Mock<IOptions<ClusterOptions>>();


            Guid nodeStorageId = Guid.NewGuid();
            var NodeStorage = new NodeStorage<TestState>(inMemoryRepository)
            {
                Id = nodeStorageId
            };

            _stateMachine.ApplyLogsToStateMachine(new List<ConsensusCore.Domain.Models.LogEntry>()
                {
                    new ConsensusCore.Domain.Models.LogEntry()
                    {
                        Commands = new List<BaseCommand>() {
                            new CreateIndex()
                        {
                            Type = "number",
                            Shards = new List<ConsensusCore.Domain.BaseClasses.SharedShardMetadata>()
                            {
                                new ConsensusCore.Domain.BaseClasses.SharedShardMetadata()
                                {
                                    Id = DefaultShardId,
                                    InsyncAllocations = new HashSet<Guid>(){ nodeStorageId },
                                    PrimaryAllocation = nodeStorageId,
                                    Type = "number",
                                    StaleAllocations = new HashSet<Guid>(),
                                    LatestOperationPos = 0
                                }
                            }
                        }
                        }
                    }
                });

            moqClusterOptions.Setup(mqo => mqo.Value).Returns(new ClusterOptions()
            {
                NodeUrls = "localhost:5022",
                TestMode = true,
                NumberOfShards = 1,
                DataTransferTimeoutMs = 1000,
                ElectionTimeoutMs = 1000,
                LatencyToleranceMs = 1000,
                MinimumNodes = 1
            });


            var manager = new ShardManager<TestState, IBaseRepository<TestState>>(_stateMachine,
                inMemoryRepository,
                factory.CreateLogger<ShardManager<TestState, IBaseRepository<TestState>>>(),
            _connector,
            _dataRouter,
            moqClusterOptions.Object,
            NodeStorage);

            manager.AddNewShardMetadata(TestUtility.DefaultShardId, "number");
            return manager;
        }
    }
}
