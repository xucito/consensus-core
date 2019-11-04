using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
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
using System.Linq;
using Xunit;

namespace ConcensusCore.Node.Tests.SingleNode
{
    public class AppendEntriesRPC_Test
    {
        public ConsensusCoreNode<TestState> Node;
        public NodeStorage<TestState> NodeStorage;

        public AppendEntriesRPC_Test()
        {
            Node = TestUtility.GetTestConsensusCoreNode();
            NodeStorage = Node._nodeStorage;
            NodeStorage.AddLogs(new System.Collections.Generic.List<LogEntry>()
                {
                    new LogEntry(){
                        Commands = new List<BaseCommand>(),
                        Index = 1,
                        Term = 5
                    },
                    new LogEntry(){
                        Commands =new List<BaseCommand>(),
                        Index = 2,
                        Term = 5
                    }
                });
            NodeStorage.CurrentTerm = 5;
        }

        [Fact]
        public void FalseIfTermIsLessThenCurrentTerm()
        {
            Assert.False((Node.Handle(new AppendEntry()
            {
                Term = 2
            }).GetAwaiter().GetResult()).IsSuccessful);
        }

        [Fact]
        public void FalseIfPrevLogIndexTermIsDifferent()
        {
            Assert.False((Node.Handle(new AppendEntry()
            {
                Term = 5,
                PrevLogIndex = 2,
                PrevLogTerm = 3
            }).GetAwaiter().GetResult()).IsSuccessful);

            Assert.False((Node.Handle(new AppendEntry()
            {
                Term = 5,
                PrevLogIndex = 2,
                PrevLogTerm = 7
            }).GetAwaiter().GetResult()).IsSuccessful);
        }

        /// <summary>
        /// Existing entry conflicts with new one (Same index but different terms) delete the existing entry and all that follows
        /// </summary>
        [Fact]
        public void NewEntryConflictsWithExistingOne()
        {
            Assert.True((Node.Handle(new AppendEntry()
            {
                Entries = new System.Collections.Generic.List<LogEntry>() {
                    new LogEntry()
                    {
                        Index = 2,
                        Term = 9
                    }
                },
                Term = 5,
                PrevLogIndex = 1,
                PrevLogTerm = 5
            }).GetAwaiter().GetResult()).IsSuccessful);


            Assert.Equal(5, NodeStorage.GetLogAtIndex(2).Term);
        }

        /// <summary>
        /// Dont append existing log
        /// </summary>
        [Fact]
        public void DontAppendExistingLog()
        {
            Assert.True((Node.Handle(new AppendEntry()
            {
                Entries = new System.Collections.Generic.List<LogEntry>() {
                    new LogEntry()
                    {
                        Index = 2,
                        Term = 5
                    }
                },
                Term = 5,
                PrevLogIndex = 1,
                PrevLogTerm = 5
            }).GetAwaiter().GetResult()).IsSuccessful);

            Assert.Equal(5, NodeStorage.GetLogAtIndex(2).Term);
        }

        [Fact]
        public void CreateSnapshot()
        {
            Assert.False(true);
        }

        [Fact]
        public void LoadSnapshotOnStartup()
        {
            Assert.False(true);
        }

        [Fact]
        public void ApplySnapshot()
        {
            Assert.False(true);
        }
    }
}
