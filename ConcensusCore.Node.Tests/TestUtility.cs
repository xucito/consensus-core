using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node;
using ConsensusCore.Node.Communication.Clients;
using ConsensusCore.Node.Communication.Controllers;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Controllers;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.Services.Tasks;
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
        //private static Guid defaultShardId;
        public static Guid DefaultShardId = new Guid("23af4254-3bf7-48b6-87bb-94b3acc64cff");

        public static IServiceProvider GetFullNodeProvider()
        {

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IOperationCacheRepository, NodeInMemoryRepository<TestState>>();
            services.AddSingleton<IBaseRepository<TestState>>(s => new NodeInMemoryRepository<TestState>());
            services.AddSingleton<IShardRepository>(s => new NodeInMemoryRepository<TestState>());
            // services.AddSingleton<NodeStorage>();
            services.AddSingleton<IStateMachine<TestState>, StateMachine<TestState>>();
            services.AddSingleton<INodeStorage<TestState>, NodeStorage<TestState>>();
            services.AddSingleton<NodeStateService>();
            services.AddSingleton<IClusterConnectionPool, ClusterConnectionPool<TestState>>();
            services.AddSingleton<ClusterClient>();
            services.AddSingleton<IRaftService, RaftService<TestState>>();
            services.AddTransient<NodeController<TestState>>();
            services.Configure<NodeOptions>(o => new NodeOptions() { });
            services.Configure<ClusterOptions>(o => new ClusterOptions() {
                TestMode = true,
                SnapshottingInterval = 20,
                SnapshottingTrailingLogCount = 10,
                ShardRecoveryValidationCount = 10
            });
            services.AddSingleton<ClusterClient>();
            services.AddSingleton<IClusterRequestHandler, ClusterRequestHandler<TestState>>();
            services.AddSingleton<IDataService, DataService<TestState>>();
            services.AddSingleton<ITaskService, TaskService<TestState>>();
            services.AddSingleton<IDataRouter, TestDataRouter>();
            var provider = services.BuildServiceProvider();
           var options = provider.GetService< IOptions<ClusterOptions>>().Value;
            options.TestMode = true;
            options.NodeUrls = "https://localhost:5021";
            provider.GetService<IRaftService>();
            return provider;
        }

        /* public static RaftService<TestState> GetTestConsensusCoreNode()
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

             var logger = factory.CreateLogger<RaftService<TestState>>();

             NodeInMemoryRepository<TestState> inMemoryRepository = new NodeInMemoryRepository<TestState>();
             var NodeStorage = new NodeStorage<TestState>(inMemoryRepository) { };
             var _dataRouter = new TestDataRouter();
             var _stateMachine = new StateMachine<TestState>();
             var _connector = new ClusterClient(TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(1000));

             return new ConsensusCoreNode<TestState>(moqClusterOptions.Object,
             moqNodeOptions.Object,
             logger,
             _stateMachine,
             inMemoryRepository,
            _connector,
             _dataRouter,
             new ShardManager<TestState, IShardRepository>(_stateMachine,
                 factory.CreateLogger<ShardManager<TestState, IShardRepository>>(),
             _connector,
             _dataRouter,
             moqClusterOptions.Object,
                 inMemoryRepository),
             NodeStorage
             );
         }*/

        public static DataService<TestState> GetTestShardManager()
        {
            var serviceProvider = new ServiceCollection()
            .AddLogging()
            .BuildServiceProvider();
            var _stateMachine = new StateMachine<TestState>()
            {

            };
            var factory = serviceProvider.GetService<ILoggerFactory>();
            NodeInMemoryRepository<TestState> inMemoryRepository = new NodeInMemoryRepository<TestState>();
            var _dataRouter = new TestDataRouter();
            var moqClusterOptions = new Mock<IOptions<ClusterOptions>>();

            var moqNodeOptions = new Mock<IOptions<NodeOptions>>();
            moqNodeOptions.Setup(mqo => mqo.Value).Returns(new NodeOptions()
            {

            });
            Guid nodeStorageId = Guid.NewGuid();
            var NodeStorage = new NodeStorage<TestState>(factory.CreateLogger<NodeStorage<TestState>>(), inMemoryRepository)
            {
                Id = nodeStorageId
            };
            var nodeStateService = new NodeStateService() {
                Id = nodeStorageId
            };
            var services = new ServiceCollection();
            var provider = services.BuildServiceProvider();
            var _connector = new ClusterClient(new ClusterConnectionPool<TestState>(_stateMachine, TimeSpan.FromMilliseconds(10000), TimeSpan.FromMilliseconds(1000)), provider, nodeStateService);

            _stateMachine.ApplyLogsToStateMachine(new List<ConsensusCore.Domain.Models.LogEntry>()
                {
                    new ConsensusCore.Domain.Models.LogEntry()
                    {
                        Commands = new List<BaseCommand>() {
                            new CreateIndex()
                        {
                            Type = "number",
                            Shards = new List<ShardAllocationMetadata>()
                            {
                                new ShardAllocationMetadata()
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


            var manager = new DataService<TestState>(
                factory,
                inMemoryRepository,
                _dataRouter,
                _stateMachine,
                nodeStateService,
                _connector,
                moqClusterOptions.Object,
                inMemoryRepository,
                moqNodeOptions.Object
                );
            return manager;
        }
    }
}
