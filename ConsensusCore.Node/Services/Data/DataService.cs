using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Data.Components;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data
{
    public class DataService<State> where State : BaseState, new()
    {
        private Task _writeTask;
        private Task _indexCreationTask;

        private readonly IShardRepository _shardRepository;
        private readonly ILogger _logger;
        private readonly WriteCache _writeCache;
        private readonly IStateMachine<State> _stateMachine;
        private readonly ClusterOptions _clusterOptions;

        //Internal Components
        private readonly Writer<State> _writer;
        private readonly Allocator _allocator;

        public DataService(
            ILoggerFactory loggerFactory,
            IShardRepository shardRepository,
            WriteCache writeCache,
            IDataRouter dataRouter,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService,
            ClusterClient clusterClient,
            IOptions<ClusterOptions> clusterOptions)
        {
            _clusterOptions = clusterOptions.Value;
            _stateMachine = stateMachine;
            _writeCache = writeCache;
            _logger = loggerFactory.CreateLogger<DataService<State>>();
            _shardRepository = shardRepository;
            _writer = new Writer<State>(loggerFactory.CreateLogger<Writer<State>>(),
                shardRepository,
                dataRouter,
                stateMachine,
                nodeStateService,
                clusterClient
                );

            _writeTask = new Task(async () =>
            {
                var operation = _writeCache.DequeueOperation();
                await _writer.WriteShardData(operation.Data, operation.Operation, operation.Id, operation.TransactionDate);
            });
            TaskUtility.RestartTask(_indexCreationTask, )
        }

        public async Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {
            try
            {
                DateTime commandStartTime = DateTime.Now;
                TResponse response;
                switch (request)
                {
                    case AddShardWriteOperation t1:
                        response = (TResponse)(object)await AddShardWriteOperationHandler(t1);
                        break;
                    case RequestCreateIndex t1:
                        response = (TResponse)(object)(CreateIndexHandler(t1));
                        break;
                    case RequestInitializeNewShard t1:
                        response = (TResponse)(object)RequestInitializeNewShardHandler(t1);
                        break;
                    case ReplicateShardOperation t1:
                        response = (TResponse)(object)await ReplicateShardOperationHandler(t1);
                        break;
                    case RequestShardOperations t1:
                        response = (TResponse)(object)await RequestShardOperationsHandler(t1);
                        break;
                    default:
                        throw new Exception("Request is not implemented");
                }

                return response;
            }
            catch (TaskCanceledException e)
            {
                Logger.LogWarning(GetNodeId() + "Request " + request.RequestName + " timed out...");
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
            catch (Exception e)
            {
                Logger.LogError(GetNodeId() + "Failed to handle request " + request.RequestName + " with error " + e.Message + Environment.StackTrace + e.StackTrace);
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
        }

        public async Task<AddShardWriteOperationResponse> AddShardWriteOperationHandler(AddShardWriteOperation shard)
        {
            Logger.LogDebug(GetNodeId() + "Received write request for object " + shard.Data.Id + " for shard " + shard.Data.ShardId);
            AddShardWriteOperationResponse finalResult = new AddShardWriteOperationResponse();
            //Check if index exists, if not - create one
            if (!_stateMachine.IndexExists(shard.Data.ShardType))
            {
                await Handle(new RequestCreateIndex()
                {
                    Type = shard.Data.ShardType
                });

                DateTime startIndexCreation = DateTime.Now;
                while (!_stateMachine.IndexExists(shard.Data.ShardType))
                {
                    if ((DateTime.Now - startIndexCreation).Milliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        throw new IndexCreationFailedException("Index creation for shard " + shard.Data.ShardType + " timed out.");
                    }
                    Thread.Sleep(100);
                }
            }

            SharedShardMetadata shardMetadata;

            if (shard.Data.ShardId == null)
            {
                var allocations = _stateMachine.GetShards(shard.Data.ShardType);
                Random rand = new Random();
                var selectedNodeIndex = rand.Next(0, allocations.Length);
                shard.Data.ShardId = allocations[selectedNodeIndex].Id;
                shardMetadata = allocations[selectedNodeIndex];
            }
            else
            {
                shardMetadata = _stateMachine.GetShard(shard.Data.ShardType, shard.Data.ShardId.Value);
            }

            //If the shard is assigned to you
            if (shardMetadata.PrimaryAllocation == _nodeStorage.Id)
            {
                finalResult = await _shardManager.WriteData(shard.Data, shard.Operation, shard.WaitForSafeWrite, shard.RemoveLock);

                if (finalResult.FailedNodes.Count() > 0)
                {
                    Logger.LogWarning("Detected invalid nodes, setting nodes " + finalResult.FailedNodes.Select(ivn => ivn.ToString()).Aggregate((i, j) => i + "," + j) + " to be out-of-sync");

                    var result = await Handle(new ExecuteCommands()
                    {
                        Commands = new List<BaseCommand>()
                            {
                                new UpdateShardMetadataAllocations()
                                {
                                    ShardId = shardMetadata.Id,
                                    Type = shardMetadata.Type,
                                    InsyncAllocationsToRemove = finalResult.FailedNodes.ToHashSet(),
                                    StaleAllocationsToAdd = finalResult.FailedNodes.ToHashSet()
                                }
                            },
                        WaitForCommits = true
                    });
                }
            }
            else
            {
                try
                {
                    await _clusterConnector.Send(shardMetadata.PrimaryAllocation, shard);
                }
                catch (Exception e)
                {
                    Logger.LogError(GetNodeId() + "Failed to write " + shard.Operation.ToString() + " request to primary node " + _stateMachine.CurrentState.Nodes[shardMetadata.PrimaryAllocation].TransportAddress + " for object " + shard.Data.Id + " shard " + shard.Data.ShardId + "|" + e.StackTrace);
                    throw e;
                }
            }

            if (shard.RemoveLock)
            {
                var result = await Handle(new ExecuteCommands()
                {
                    Commands = new List<BaseCommand>()
                        {
                            new RemoveObjectLock()
                            {
                                ObjectId = shard.Data.Id,
                                Type = shardMetadata.Type
                            }
                },
                    WaitForCommits = true
                });

                if (result.IsSuccessful)
                {
                    finalResult.LockRemoved = true;
                }
            }

            return finalResult;
        }
    }
}
