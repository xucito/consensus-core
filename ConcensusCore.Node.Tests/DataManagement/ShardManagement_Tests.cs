using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.Services.Data;
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
        public DataService<TestState> Node;

        public ShardManagement_Tests()
        {
            Node = TestUtility.GetTestShardManager();
        }

        [Fact]
        public async void WriteNewDataType()
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
        }


        [Fact]
        public async void ThrowErrorWhenNumberOfShardsIs0()
        {
            Assert.True(false);
        }


        [Fact]
        public async void GetData()
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
            var dataResult = await Node.Handle(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.Null(dataResult.Data);
            Assert.Equal("The type number does not exist.", dataResult.SearchMessage);

            //Write a a new operation to create the index
            var result = await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData
                {
                    Id = Guid.NewGuid(),
                    Data = 1,
                    ShardType = "number"
                }
            });

            dataResult = await Node.Handle(new RequestDataShard()
            {
                Type = "number",
                ObjectId = objectId
            });

            Assert.True(dataResult.IsSuccessful);
            Assert.Null(dataResult.Data);
            Assert.Equal("Object " + objectId + " could not be found in shards.", dataResult.SearchMessage);
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
        public async void RequestShardWriteOperationsHandler()
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

            var requestShardResult = await Node.Handle(new RequestShardWriteOperations()
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
            var result = await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData
                {
                    Id = objectId,
                    Data = 1,
                    ShardType = "number"
                }
            });

            var updateResult = await Node.Handle(new AddShardWriteOperation()
            {
                Data = new TestData()
                {
                    Id = objectId,
                    Data = 2,
                    ShardType = "number"
                },
                Operation = ShardOperationOptions.Update,
            });

            var dataResult = await Node.Handle(new RequestDataShard()
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
