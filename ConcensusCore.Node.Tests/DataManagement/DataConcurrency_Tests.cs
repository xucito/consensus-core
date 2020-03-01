using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node.Communication.Controllers;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.TestNode.Models;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ConcensusCore.Node.Tests.DataManagement
{
    public class DataConcurrency_Tests
    {
        public IClusterRequestHandler Node;

        public DataConcurrency_Tests()
        {
            Node = TestUtility.GetFullNodeProvider().GetService<IClusterRequestHandler>();
        }

        [Fact]
        public async void LockDataOnGet()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Handle(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.NotNull(dataResult.Data);
            Assert.Equal(1, ((TestData)dataResult.Data).Data);
            Assert.True(dataResult.AppliedLocked);

            var failedRequest = await Node.Handle(new RequestDataShard()
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

            await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            var result = await Node.Handle(new AddShardWriteOperation()
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
                var sendRequest = await Node.Handle(new RequestDataShard()
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
        public async void ReleaseLockOnUpdate()
        {
            var objectId = Guid.NewGuid();
            var result = await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Handle(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            Assert.True((await Node.Handle(new AddShardWriteOperation()
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

            Assert.True((await Node.Handle(new RequestDataShard()
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
            var result = await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            Assert.True(result.IsSuccessful);

            var dataResult = await Node.Handle(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId,
                CreateLock = true
            });

            Assert.True((await Node.Handle(new ExecuteCommands()
            {
                Commands = new List<BaseCommand>(){ new RemoveLock()
                {
                    Name = dataResult.Data.GetLockName(),
                    LockId = dataResult.LockId.Value
                }},
                WaitForCommits = true
            })).IsSuccessful);

            Assert.True((await Node.Handle(new RequestDataShard()
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
                Node.Handle(new AddShardWriteOperation()
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
                Node.Handle(new AddShardWriteOperation()
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
            //Assert.Single(Node.LocalShards);
        }
    }


}
