using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.TestNode.Models;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ConcensusCore.Node.Tests.DataManagement
{
    public class ShardManager_Tests
    {
        ShardManager<TestState, IShardRepository> _shardManager { get; set; }

        public ShardManager_Tests()
        {
            _shardManager = TestUtility.GetTestShardManager();
        }

        [Fact]
        public async void CreateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(100, ((TestData)result.Data).Data);
        }

        [Fact]
        public async void ReplicateCreateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.ReplicateShardOperation(new ShardOperation()
            {
                ObjectId = recordId,
                Operation = ShardOperationOptions.Create,
                Pos = 1,
                ShardId = TestUtility.DefaultShardId
            },
                new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            });

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(100, ((TestData)result.Data).Data);
        }

        [Fact]
        public async void ReplicateOperation()
        {
            Guid recordId = Guid.NewGuid();

            var replicationResult = await _shardManager.ReplicateShardOperation(new ShardOperation()
            {
                ObjectId = recordId,
                Operation = ShardOperationOptions.Create,
                Pos = 1,
                ShardId = TestUtility.DefaultShardId
            }, new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            });

            Assert.True(replicationResult.IsSuccessful);



            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(100, ((TestData)result.Data).Data);

            Guid record2 = Guid.NewGuid();
            var replicationResult2 = await _shardManager.ReplicateShardOperation(new ShardOperation()
            {
                ObjectId = record2,
                Operation = ShardOperationOptions.Create,
                Pos = 2,
                ShardId = TestUtility.DefaultShardId
            }, new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 300,
                Id = record2
            });


            var result2 = await _shardManager.RequestDataShard(record2, "number", 3000);

            Assert.True(result2.IsSuccessful);
            Assert.NotNull(result2.Data);
            Assert.Equal(300, ((TestData)result2.Data).Data);
        }

        [Fact]
        public async void RevertCreateOperation()
        {
            Guid recordId = Guid.NewGuid();

            var writeResult = await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            var oldSyncPos = _shardManager.GetShardLocalMetadata(writeResult.ShardId).SyncPos;
            _shardManager.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", writeResult.Pos.Value, new Dictionary<Guid, ShardData> { });
            var newResult = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.Equal(oldSyncPos - 1, _shardManager.GetShardLocalMetadata(writeResult.ShardId).SyncPos);
            Assert.Null(newResult.Data);
        }

        [Fact]
        public async void UpdateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            await _shardManager.WriteData(new TestData()
            {
                Id = recordId,
                ShardType = "number",
                Data = 200,
                ShardId = TestUtility.DefaultShardId
            }, ShardOperationOptions.Update, true);

            var updatedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.Equal(200, ((TestData)updatedResult.Data).Data);
        }

        [Fact]
        public async void ReplicateUpdateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            await _shardManager.ReplicateShardOperation(new ShardOperation()
            {
                ObjectId = recordId,
                Operation = ShardOperationOptions.Update,
                Pos = 2,
                ShardId = TestUtility.DefaultShardId
            },
            new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 101,
                Id = recordId
            });

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

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

            await _shardManager.ReplicateShardOperation(new ShardOperation()
            {
                ObjectId = recordId,
                Operation = ShardOperationOptions.Create,
                Pos = 1,
                ShardId = TestUtility.DefaultShardId
            },
                new TestData()
                {
                    ShardId = TestUtility.DefaultShardId,
                    ShardType = "number",
                    Data = 100,
                    Id = recordId
                });

            await _shardManager.ReplicateShardOperation(new ShardOperation()
            {
                ObjectId = recordId,
                Operation = ShardOperationOptions.Update,
                Pos = 2,
                ShardId = TestUtility.DefaultShardId
            },
            new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 101,
                Id = recordId
            });

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.True(result.IsSuccessful);
            Assert.NotNull(result.Data);
            Assert.Equal(101, ((TestData)result.Data).Data);
        }

        [Fact]
        public async void RevertUpdateOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            var result = await _shardManager.RequestDataShard(recordId, "number", 3000);

            var updateWriteResult = await _shardManager.WriteData(new TestData()
            {
                Id = recordId,
                ShardType = "number",
                Data = 200,
                ShardId = TestUtility.DefaultShardId
            }, ShardOperationOptions.Update, true);

            var updatedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);
            var oldSyncPos = _shardManager.GetShardLocalMetadata(updateWriteResult.ShardId).SyncPos;

            Assert.Equal(200, ((TestData)updatedResult.Data).Data);

            _shardManager.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", updateWriteResult.Pos.Value, new Dictionary<Guid, ShardData>() {
                { recordId,  new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            } } });
            Assert.Equal(oldSyncPos - 1, _shardManager.GetShardLocalMetadata(updateWriteResult.ShardId).SyncPos);
            var revertedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);
            Assert.Equal(100, ((TestData)revertedResult.Data).Data);
        }

        [Fact]
        public async void DeleteOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            await _shardManager.WriteData(new TestData() { Id = recordId, ShardId = TestUtility.DefaultShardId, ShardType = "number" }, ShardOperationOptions.Delete, true);

            var updatedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.Null(updatedResult.Data);

        }
        [Fact]
        public async void RevertDeleteOperation()
        {
            Guid recordId = Guid.NewGuid();

            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            var deleteDataResponse = await _shardManager.WriteData(new TestData() { Id = recordId, ShardId = TestUtility.DefaultShardId, ShardType = "number" }, ShardOperationOptions.Delete, true);
            var updatedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);

            Assert.True(updatedResult.IsSuccessful);
            var oldSyncPos = _shardManager.GetShardLocalMetadata(deleteDataResponse.ShardId).SyncPos;
            _shardManager.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", deleteDataResponse.Pos.Value, new Dictionary<Guid, ShardData>() {
                { recordId,  new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            } } });
            Assert.Equal(oldSyncPos - 1, _shardManager.GetShardLocalMetadata(deleteDataResponse.ShardId).SyncPos);
            var revertedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);
            Assert.Equal(100, ((TestData)revertedResult.Data).Data);
        }

        [Fact]
        public async void RevertBadTransaction()
        {
            Guid firstRecordId = Guid.NewGuid();
            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = firstRecordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            Guid secondRecordId = Guid.NewGuid();
            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 101,
                Id = firstRecordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);
        }

        [Fact]
        public async void RequestOperations()
        {
            Guid firstRecordId = Guid.NewGuid();
            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = firstRecordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            Guid secondRecordId = Guid.NewGuid();
            await _shardManager.WriteData(new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 101,
                Id = firstRecordId
            }, ConsensusCore.Domain.Enums.ShardOperationOptions.Create, true);

            var operations = await _shardManager.RequestShardOperations(TestUtility.DefaultShardId, 1, 2, "number", true);

            Assert.Equal(2, operations.Operations.Count);
            //Check ascending
            Assert.Equal(1, operations.Operations[1].Position);
            Assert.Equal(2, operations.Operations[2].Position);

            var limitOperations = await _shardManager.RequestShardOperations(TestUtility.DefaultShardId, 1, 1, "number", true);

            Assert.Single(limitOperations.Operations);
        }
    }
}
