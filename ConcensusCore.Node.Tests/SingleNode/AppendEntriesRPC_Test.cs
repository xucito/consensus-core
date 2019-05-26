using ConsensusCore.Node;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Linq;
using Xunit;

namespace ConcensusCore.Node.Tests.SingleNode
{
    public class AppendEntriesRPC_Test
    {
        public ConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>> Node;
        public NodeStorage<TestCommand, NodeInMemoryRepository<TestCommand>> NodeStorage;

        public AppendEntriesRPC_Test()
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

            NodeStorage = new ConsensusCore.Node.Services.NodeStorage<TestCommand, NodeInMemoryRepository<TestCommand>>(new NodeInMemoryRepository<TestCommand>())
            {
                CurrentTerm = 5,
                Logs = new System.Collections.Generic.List<LogEntry<TestCommand>>()
                {
                    new LogEntry<TestCommand>(){
                        Commands = new System.Collections.Generic.List<TestCommand>(){ },
                        Index = 1,
                        Term = 5
                    },
                    new LogEntry<TestCommand>(){
                        Commands = new System.Collections.Generic.List<TestCommand>(){ },
                        Index = 2,
                        Term = 5
                    }
                }
            };

            var inMemoryRepository = new NodeInMemoryRepository<TestCommand>();
            Node = new ConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>>(moqClusterOptions.Object,
            moqNodeOptions.Object,
            NodeStorage,
            logger, new ConsensusCore.Node.Interfaces.StateMachine<TestCommand, TestState>())
            {
                IsBootstrapped = true
            };

        }

        [Fact]
        public void FalseIfTermIsLessThenCurrentTerm()
        {
            Assert.False(Node.AppendEntry(new AppendEntry<TestCommand>()
            {
                Term = 2
            }));
        }

        [Fact]
        public void FalseIfPrevLogIndexTermIsDifferent()
        {
            //Less then current term
            Assert.False(Node.AppendEntry(new AppendEntry<TestCommand>()
            {
                Term = 5,
                PrevLogIndex = 2,
                PrevLogTerm = 3
            }));

            //Greater then current term
            Assert.False(Node.AppendEntry(new AppendEntry<TestCommand>()
            {
                Term = 5,
                PrevLogIndex = 2,
                PrevLogTerm = 7
            }));
        }

        /// <summary>
        /// Existing entry conflicts with new one (Same index but different terms) delete the existing entry and all that follows
        /// </summary>
        [Fact]
        public void NewEntryConflictsWithExistingOne()
        {
            //Less then current term
            Assert.True(Node.AppendEntry(new AppendEntry<TestCommand>()
            {
                Entries = new System.Collections.Generic.List<LogEntry<TestCommand>>() {
                    new LogEntry<TestCommand>()
                    {
                        Index = 2,
                        Term = 9
                    }
                },
                Term = 5,
                PrevLogIndex = 1,
                PrevLogTerm = 5
            }));

            Assert.Equal(2, NodeStorage.Logs.Count());
            Assert.Equal(2, NodeStorage.Logs.Last().Index);
            Assert.Equal(9, NodeStorage.Logs.Last().Term);
        }

        /// <summary>
        /// Dont append existing log
        /// </summary>
        [Fact]
        public void DontAppendExistingLog()
        {
            //Less then current term
            Assert.True(Node.AppendEntry(new AppendEntry<TestCommand>()
            {
                Entries = new System.Collections.Generic.List<LogEntry<TestCommand>>() {
                    new LogEntry<TestCommand>()
                    {
                        Index = 2,
                        Term = 9
                    }
                },
                Term = 5,
                PrevLogIndex = 1,
                PrevLogTerm = 5
            }));

            Assert.Equal(2, NodeStorage.Logs.Count());
            Assert.Equal(2, NodeStorage.Logs.Last().Index);
            Assert.Equal(9, NodeStorage.Logs.Last().Term);
        }
    }
}
