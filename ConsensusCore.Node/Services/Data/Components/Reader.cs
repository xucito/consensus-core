using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data.Components
{
    public class Reader<State> where State : BaseState, new()
    {
        private readonly IDataRouter _dataRouter;
        private readonly IShardRepository _shardRepository;
        private readonly IStateMachine<State> _stateMachine;
        private readonly NodeStateService _nodeStateService;
        private readonly ClusterClient _clusterClient;
        private readonly ILogger _logger;

        public Reader(
            ILogger<Reader<State>> logger,
            IShardRepository shardRepository,
            IDataRouter dataRouter,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient)
        {
            _logger = logger;
            _dataRouter = dataRouter;
            _shardRepository = shardRepository;
            _stateMachine = stateMachine;
            _nodeStateService = nodeStateService;
            _clusterClient = clusterClient;
        }

        public async Task<ShardData> GetData(Guid objectId, string type, int timeoutMs, Guid? shardId = null)
        {
            Guid? FoundShard = null;
            Guid? FoundOnNode = null;
            var currentTime = DateTime.Now;

            if (shardId == null)
            {
                var shards = _stateMachine.GetAllPrimaryShards(type);

                bool foundResult = false;
                ShardData finalObject = null;
                var totalRespondedShards = 0;

                var tasks = shards.Select(async shard =>
                {
                    if (shard.Value != _nodeStateService.Id)
                    {
                        try
                        {
                            var result = await _clusterClient.Send(shard.Value, new RequestDataShard()
                            {
                                ObjectId = objectId,
                                ShardId = shard.Key, //Set the shard
                                Type = type
                            });

                            if (result.IsSuccessful)
                            {
                                foundResult = true;
                                finalObject = result.Data;
                                FoundShard = result.ShardId;
                                FoundOnNode = result.NodeId;
                            }

                            Interlocked.Increment(ref totalRespondedShards);
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(_nodeStateService.GetNodeLogId() + "Error thrown while getting " + e.Message);
                        }
                    }
                    else
                    {
                        finalObject = await _dataRouter.GetDataAsync(type, objectId);
                        foundResult = finalObject != null ? true : false;
                        FoundShard = shard.Key;
                        FoundShard = shard.Value;
                        Interlocked.Increment(ref totalRespondedShards);
                    }
                });

                //Don't await, this will trigger the tasks
                Task.WhenAll(tasks);

                while (!foundResult && totalRespondedShards < shards.Count)
                {
                    if ((DateTime.Now - currentTime).TotalMilliseconds > timeoutMs)
                    {
                        throw new ClusterOperationTimeoutException("Get data request for object " + objectId + " from shard " + shardId + " timed out.");
                    }
                    Thread.Sleep(10);
                }

                return finalObject;
            }
            else
            {
                return await _dataRouter.GetDataAsync(type, objectId);
            }
        }
    }
}
