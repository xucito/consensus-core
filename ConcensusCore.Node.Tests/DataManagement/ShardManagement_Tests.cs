using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.TestNode.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ConcensusCore.Node.Tests.DataManagement
{
    public class ShardManagement_Tests
    {
        public ConsensusCoreNode<TestState, NodeInMemoryRepository> Node;
        public IDataRouter _dataRouter;
        public NodeStorage NodeStorage;

        public ShardManagement_Tests()
        {
            var moqClusterOptions = new Mock<IOptions<ClusterOptions>>();
            moqClusterOptions.Setup(mqo => mqo.Value).Returns(new ClusterOptions()
            {
                NodeUrls =  "localhost:5022",
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

            var logger = factory.CreateLogger<ConsensusCoreNode<TestState, NodeInMemoryRepository>>();

            var inMemoryRepository = new NodeInMemoryRepository();
            NodeStorage = new NodeStorage(inMemoryRepository);
            _dataRouter = new TestDataRouter();

            Node = new ConsensusCoreNode<TestState, NodeInMemoryRepository>(moqClusterOptions.Object,
            moqNodeOptions.Object, logger,
            new StateMachine<TestState>(), inMemoryRepository, new TestDataRouter())
            {
                IsBootstrapped = true
            };

            while (!Node.InCluster || Node.CurrentState != NodeState.Leader)
            {
                Thread.Sleep(1000);
            }
        }

        [Fact]
        public async void WriteNewDataType()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);
        }

        [Fact]
        public async void ReleaseLockOnUpdate()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            Assert.True((await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    ShardType = "number",
                    Data = 2
                },
                Operation = ShardOperationOptions.Update,
                WaitForSafeWrite = true,
                RemoveLock = true
            })).IsSuccessful);

            Assert.True((await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            })).IsSuccessful);

        }

        [Fact]
        public async void RemoveLockCommand()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            Assert.True((await Node.Send(new ExecuteCommands()
            {
                Commands = new List<BaseCommand>(){ new RemoveObjectLock()
                {
                    ObjectId = objectId,
                    Type = "number"
                }
                }
            })).IsSuccessful);

            Assert.True((await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            })).IsSuccessful);
        }

        [Fact]
        public async void ConcurrentNewDataWrites()
        {
            var objectId = Guid.NewGuid();
            var tasks = new List<Task>();
            tasks.Add(
                Node.Send(new WriteData()
                {
                    Data = new TestData
                    {
                        Id = objectId,
                        Data = 1,
                        ShardType = "number"
                    }
                })
            );

            tasks.Add(
                Node.Send(new WriteData()
                {
                    Data = new TestData
                    {
                        Data = 1,
                        ShardType = "number"
                    }
                })
            );

            await Task.WhenAll(tasks);
            //Check there is only one shard created
            Assert.Single(Node.LocalShards);
        }

        [Fact]
        public async void GetData()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.NotNull(dataResult.Data);
            Assert.Equal(1, ((TestData)dataResult.Data).Data);
        }

        [Fact]
        public async void ReturnNullOnMissingDataOnGetData()
        {
            var objectId = Guid.NewGuid();

            //This will check the request when there is no shard at all
            var dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.Null(dataResult.Data);
            Assert.Equal("The type number does not exist.", dataResult.SearchMessage);

            //Write a a new operation to create the index
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = Guid.NewGuid(),
                    Data = 1,
                    ShardType = "number"
                }
            });

            dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.Null(dataResult.Data);
            Assert.Equal("Object " + objectId + " could not be found in shards.", dataResult.SearchMessage);
        }

        [Fact]
        public async void LockDataOnGet()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.NotNull(dataResult.Data);
            Assert.Equal(1, ((TestData)dataResult.Data).Data);

            var failedRequest = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            //Request should still be successful
            Assert.True(failedRequest.IsSuccessful);
            Assert.Null(failedRequest.Data);
            Assert.Equal("Object " + objectId + " is locked.", failedRequest.SearchMessage);
        }

        [Fact]
        public async void HandleConcurrentLockRequests()
        {
            var objectId = Guid.NewGuid();

            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });
            var tests = new int[] { 1, 2 };

            int foundResults = 0;

            var runningTests = tests.Select(async t =>
             {
                 var sendRequest = await Node.Send(new RequestDataShard()
                 {
                     Type = "number",
                     ObjectId = objectId,
                     CreateLock = true
                 });

                 if (sendRequest.IsSuccessful && sendRequest.Data != null)
                 {
                     Interlocked.Increment(ref foundResults);
                 }
             });

            await Task.WhenAll(runningTests);

            Assert.Equal(1, foundResults);
        }

        [Fact]
        public void DeleteData()
        {

        }

        [Fact]
        public void CreateShard()
        {

        }

        [Fact]
        public void MarkIndexAsStaleAfterFailureToWrite()
        {

        }

        [Fact]
        public void LeaderReassignPrimaryOnNodeFailure()
        {

        }

        [Fact]
        public async void RequestShardOperationsHandler()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            var requestShardResult = await Node.Send(new RequestShardOperations()
            {
                From = 0,
                To = 1,
                ShardId = result.ShardId,
                Type = "number"
            });

            Assert.Single(requestShardResult.Operations);
        }

        [Fact]
        public async void UpdateData()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Send(new WriteData()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            var updateResult = await Node.Send(new WriteData()
            {
                Data = new TestData()
                {
                    Id = objectId,
                    Data = 2,
                    ShardType = "number"
                },
                Operation = ShardOperationOptions.Update,
            });

            var dataResult = await Node.Send(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId
            });


            Assert.True(dataResult.IsSuccessful);
            Assert.NotNull(dataResult.Data);
            Assert.Equal(2, ((TestData)dataResult.Data).Data);
        }
    }
}
