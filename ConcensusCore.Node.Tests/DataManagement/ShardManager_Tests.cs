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
        ShardManager<TestState, IBaseRepository> _shardManager { get; set; }

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

            _shardManager.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", writeResult.Pos.Value);

            var newResult = await _shardManager.RequestDataShard(recordId, "number", 3000);

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

            Assert.Equal(200, ((TestData)updatedResult.Data).Data);

            _shardManager.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", updateWriteResult.Pos.Value, new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            });

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

            var writeDataResponse = await _shardManager.WriteData(new TestData() { Id = recordId, ShardId = TestUtility.DefaultShardId, ShardType = "number" }, ShardOperationOptions.Delete, true);

            var updatedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);

            _shardManager.ReverseLocalTransaction(TestUtility.DefaultShardId, "number", writeDataResponse.Pos.Value, new TestData()
            {
                ShardId = TestUtility.DefaultShardId,
                ShardType = "number",
                Data = 100,
                Id = recordId
            });

            var revertedResult = await _shardManager.RequestDataShard(recordId, "number", 3000);
            Assert.Equal(100, ((TestData)revertedResult.Data).Data);
        }
    }
}
