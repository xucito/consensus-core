using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.Utility;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.TestNode.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace ConcensusCore.Node.Tests.DataManagement
{
    public class ShardManager_Tests
    {
        DataService<TestState> _shardManager { get; set; }

        public ShardManager_Tests()
        {
            _shardManager = TestUtility.GetTestShardManager();
        }

        [Fact]
        public async void CreateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                },
                Operation = ShardOperationOptions.Create
            });

            var result = await _shardManager.Handle(
                new RequestDataShard()
                {
                    ObjectId = recordId,
                    Type = "number",

                });

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(100, ((TestData)result.Data).Data);
        }

        [Fact]
        public async void ReplicateCreateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.Handle(new ReplicateShardWriteOperation()
            {
                Operation =
                new ShardWriteOperation()
                {
                    Id = Guid.NewGuid().ToString(),
                    Operation = ShardOperationOptions.Create,
                    Data = new TestData()
                    {
                        ShardId = TestUtility.DefaultShardId,
                        ShardType = "number",
                        Data = 100,
                        Id = recordId
                    },
                    Pos = 1,
                    ShardHash = ObjectUtility.HashStrings("", recordId.ToString())
                }
            });

            var result = await _shardManager.Handle(
                new RequestDataShard()
                {
                    ObjectId = recordId,
                    Type = "number",

                });

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(100, ((TestData)result.Data).Data);
        }

        [Fact]
        public async void ReplicateOperation()
        {
            Guid recordId = Guid.NewGuid();
            string firstTransactionId = Guid.NewGuid().ToString();
            var replicationResult = await _shardManager.Handle(
                new ReplicateShardWriteOperation()
                {
                    Operation = new ShardWriteOperation()
                    {
                        Id = firstTransactionId,
                        Operation = ShardOperationOptions.Create,
                        Pos = 1,
                        Data = new TestData()
                        {
                            ShardId = TestUtility.DefaultShardId,
                            ShardType = "number",
                            Data = 100,
                            Id = recordId
                        },
                        ShardHash = ObjectUtility.HashStrings("", firstTransactionId)
                    }
                });

            Assert.True(replicationResult.IsSuccessful);

            var result = await _shardManager.Handle(
                new RequestDataShard()
                {
                    ObjectId = recordId,
                    Type = "number"
                });

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(100, ((TestData)result.Data).Data);

            Guid record2 = Guid.NewGuid();
            string transaction2 = Guid.NewGuid().ToString();
            var replicationResult2 = await _shardManager.Handle(new ReplicateShardWriteOperation()
            {
                Operation =
                new ShardWriteOperation()
                {
                    Id = transaction2,
                    Operation = ShardOperationOptions.Create,
                    Pos = 2,
                    Data = new TestData()
                    {
                        ShardId = TestUtility.DefaultShardId,
                        ShardType = "number",
                        Data = 300,
                        Id = record2
                    },
                    ShardHash = ObjectUtility.HashStrings(ObjectUtility.HashStrings("", firstTransactionId), transaction2)
                }
            });

            Assert.True(replicationResult2.IsSuccessful);

            var result2 = await _shardManager.Handle(new RequestDataShard()
            {
                ObjectId = record2,
                Type = "number"
            });

            Assert.True(result2.IsSuccessful);
            Assert.NotNull(result2.Data);
            Assert.Equal(300, ((TestData)result2.Data).Data);
        }

        [Fact]
        public async void RevertCreateOperation()
        {
            Guid recordId = Guid.NewGuid();

            var writeResult = await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                }
            });

            var result = await _shardManager.Handle(new RequestShardWriteOperations()
            {
                From = 1,
                To = 1,
                Type = "number",
                ShardId = TestUtility.DefaultShardId
            });
            _shardManager.Syncer.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", result.Operations.First().Value.Pos);
            var newResult = await _shardManager.Reader.GetData(recordId, "number", 3000);
            Assert.Null(newResult);
        }

        [Fact]
        public async void UpdateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            var result = await _shardManager.Reader.GetData(recordId, "number", 3000);

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 200,
                    Id = recordId
                },
                Operation = ShardOperationOptions.Update
            });

            var updatedResult = await _shardManager.Handle(new RequestDataShard()
            {
                ObjectId = recordId,
                Type = "number"
            });

            Assert.Equal(200, ((TestData)updatedResult.Data).Data);
        }

        [Fact]
        public async void ReplicateUpdateOperation()
        {
            Guid recordId = Guid.NewGuid();

            var firstTransaction = await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            var secondId = Guid.NewGuid().ToString();
            await _shardManager.Handle(new ReplicateShardWriteOperation()
            {
                Operation = new ShardWriteOperation()
                {
                    Id = secondId,
                    Operation = ShardOperationOptions.Update,
                    Pos = 2,
                    Data = new TestData()
                    {
                        ShardId = TestUtility.DefaultShardId,
                        ShardType = "number",
                        Data = 101,
                        Id = recordId
                    },
                    ShardHash = ObjectUtility.HashStrings(firstTransaction.ShardHash, secondId)
                }
            });

            var result = await _shardManager.Handle(new RequestDataShard()
            {
                ObjectId = recordId,
                Type = "number"
            });

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(101, ((TestData)result.Data).Data);
        }

        /// <summary>
        /// Test a create then replicate
        /// </summary>
        [Fact]
        public async void ConsecutiveReplicationTest()
        {
            Guid recordId = Guid.NewGuid();
            string transactionId = Guid.NewGuid().ToString();
           var firstTransaction =  await _shardManager.Handle(new ReplicateShardWriteOperation()
            {
                Operation = new ShardWriteOperation()
                {
                    Id = transactionId,
                    Operation = ShardOperationOptions.Create,
                    Pos = 1,
                    Data = new TestData()
                    {
                        ShardId = TestUtility.DefaultShardId,
                        ShardType = "number",
                        Data = 100,
                        Id = recordId
                    },
                    ShardHash = ObjectUtility.HashStrings("", transactionId)
                }
            });

            var secondTransactionId = Guid.NewGuid().ToString();
            await _shardManager.Handle(new ReplicateShardWriteOperation()
            {
                Operation = new ShardWriteOperation()
                {
                    Id = secondTransactionId,
                    Operation = ShardOperationOptions.Update,
                    Pos = 2,
                    Data =
                new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 101,
                    Id = recordId
                },
                    ShardHash = ObjectUtility.HashStrings(ObjectUtility.HashStrings("", transactionId), secondTransactionId)
                }
            });

            var result = await _shardManager.Handle(new RequestDataShard()
            {
                ObjectId = recordId,
                Type = "number"
            });

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(101, ((TestData)result.Data).Data);
        }

        [Fact]
        public async void RevertUpdateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                },
                Operation = ShardOperationOptions.Create
            });

            var result = await _shardManager.Handle(new RequestDataShard() { ObjectId = recordId, Type = "number" });

            var updateWriteResult = await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    Id = recordId,
                    ShardType = "number",
                    Data = 200,
                    ShardId = TestUtility.DefaultShardId
                },
                Operation =
                ShardOperationOptions.Update
            });

            var updatedResult = await _shardManager.Handle(new RequestDataShard() { ObjectId = recordId, Type = "number" });

            Assert.Equal(200, ((TestData)updatedResult.Data).Data);

            _shardManager.Syncer.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", updateWriteResult.Pos.Value);
            var revertedResult = await _shardManager.Handle(new RequestDataShard() { ObjectId = recordId, Type = "number" });
            Assert.Equal(100, ((TestData)revertedResult.Data).Data);
        }

        [Fact]
        public async void DeleteOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData() { Id = recordId, ShardId = TestUtility.DefaultShardId, ShardType = "number" },
                Operation = ShardOperationOptions.Delete
            });

            var updatedResult = await _shardManager.Handle(new RequestDataShard() { ObjectId = recordId, Type = "number" });

            Assert.Null(updatedResult.Data);

        }
        [Fact]
        public async void RevertDeleteOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            var deleteDataResponse = await _shardManager.Handle(
                new AddShardWriteOperation()
                {
                    Data = new TestData() { Id = recordId, ShardId = TestUtility.DefaultShardId, ShardType = "number" },
                    Operation =
                    ShardOperationOptions.Delete
                });
            var updatedResult = await _shardManager.Handle(new RequestDataShard() { ObjectId = recordId, Type = "number" });

            Assert.True(updatedResult.IsSuccessful);
            _shardManager.Syncer.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", deleteDataResponse.Pos.Value);
            var revertedResult = await _shardManager.Handle(new RequestDataShard() { ObjectId = recordId, Type = "number" });
            Assert.Equal(100, ((TestData)revertedResult.Data).Data);
        }

        [Fact]
        public async void RevertBadTransaction()
        {
            Guid firstRecordId = Guid.NewGuid();
            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = firstRecordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            Guid secondRecordId = Guid.NewGuid();
            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 101,
                    Id = firstRecordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });
        }

        [Fact]
        public async void RequestOperations()
        {
            Guid firstRecordId = Guid.NewGuid();
            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = firstRecordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            Guid secondRecordId = Guid.NewGuid();
            await _shardManager.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 101,
                    Id = firstRecordId
                },
                Operation = ConsensusCore.Domain.Enums.ShardOperationOptions.Create
            });

            var operations = await _shardManager.Handle(new RequestShardWriteOperations()
            {
                ShardId = TestUtility.DefaultShardId,
                From = 1,
                To = 2,
                Type = "number"
            });

            Assert.Equal(2, operations.Operations.Count);
            //Check ascending
            Assert.Equal(1, operations.Operations[1].Pos);
            Assert.Equal(2, operations.Operations[2].Pos);

            var limitOperations = await _shardManager.Handle(new RequestShardWriteOperations()
            {
                ShardId = TestUtility.DefaultShardId,
                From = 1,
                To = 1,
                Type = "number"
            });

            Assert.Single(limitOperations.Operations);
        }
    }
}
