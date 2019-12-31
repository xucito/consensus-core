using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ConsensusCore.Node.Services.Data.Components
{
    public class Allocator<State> where State : BaseState, new()
    {
        private readonly IDataRouter _dataRouter;
        private readonly IShardRepository _shardRepository;
        private readonly IStateMachine<State> _stateMachine;
        private readonly NodeStateService _nodeStateService;
        private readonly ClusterClient _clusterClient;
        private readonly ILogger _logger;

        public Allocator(
            ILogger<Allocator<State>> logger,
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

        public async void CreateIndex(string type, int dataTransferTimeoutMs, int numberOfShards)
        {
            bool successfulAllocation = false;
            while (!successfulAllocation)
            {
                try
                {
                    //This is for the primary copy
                    var eligbleNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Value.IsContactable).ToDictionary(k => k.Key, v => v.Value);
                    var rand = new Random();
                    DateTime startTime = DateTime.Now;
                    while (eligbleNodes.Count() == 0)
                    {
                        if ((DateTime.Now - startTime).TotalMilliseconds > dataTransferTimeoutMs)
                        {
                            _logger.LogError("Failed to create indext type " + type + " request timed out...");
                            throw new ClusterOperationTimeoutException("Failed to create indext type " + type + " request timed out...");
                        }
                        _logger.LogWarning(GetNodeId() + "No eligible nodes found, awaiting eligible nodes.");
                        Thread.Sleep(1000);
                        eligbleNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Value.IsContactable).ToDictionary(k => k.Key, v => v.Value);
                    }

                    List<SharedShardMetadata> Shards = new List<SharedShardMetadata>();

                    for (var i = 0; i < numberOfShards; i++)
                    {
                        Shards.Add(new SharedShardMetadata()
                        {
                            InsyncAllocations = eligbleNodes.Keys.ToHashSet(),
                            PrimaryAllocation = eligbleNodes.ElementAt(rand.Next(0, eligbleNodes.Count())).Key,
                            Id = Guid.NewGuid(),
                            Type = type
                        });

                        foreach (var allocationI in Shards[i].InsyncAllocations)
                        {
                            if (allocationI != _nodeStateService.Id)
                            {
                                await _clusterClient.Send(allocationI, new RequestInitializeNewShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                });
                            }
                            else
                            {
                                await Handle(new RequestInitializeNewShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                });
                            }
                        }
                    }

                    var result = await Handle(new ExecuteCommands()
                    {
                        Commands = new List<CreateIndex>() {
                                new CreateIndex() {
                                        Type = type,
                                        Shards = Shards
                                }
                            },
                        WaitForCommits = true
                    });
                    successfulAllocation = true;
                }
                catch (Exception e)
                {
                    Logger.LogDebug(GetNodeId() + "Error while assigning primary node " + e.StackTrace);
                }
            }
        }
    }
}
