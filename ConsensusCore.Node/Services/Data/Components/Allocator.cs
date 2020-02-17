using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.Services.Tasks.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

        public async Task<bool> CreateIndexAsync(string type, int dataTransferTimeoutMs, int numberOfShards)
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
                        _logger.LogWarning(_nodeStateService.GetNodeLogId() + "No eligible nodes found, awaiting eligible nodes.");
                        Thread.Sleep(1000);
                        eligbleNodes = _stateMachine.CurrentState.Nodes.Where(n => n.Value.IsContactable).ToDictionary(k => k.Key, v => v.Value);
                    }

                    List<ShardAllocationMetadata> Shards = new List<ShardAllocationMetadata>();

                    for (var i = 0; i < numberOfShards; i++)
                    {
                        Shards.Add(new ShardAllocationMetadata()
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
                                await _clusterClient.Send(allocationI, new AllocateShard()
                                {
                                    ShardId = Shards[i].Id,
                                    Type = type
                                });
                            }
                            else
                            {
                                AllocateShard(Shards[i].Id, type);
                            }
                        }
                    }

                    var result = await _clusterClient.Send(_nodeStateService.CurrentLeader.Value, new ExecuteCommands()
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
                    _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Error while assigning primary node " + e.StackTrace);
                    return false;
                }
            }
            return true;
        }

        public async void AllocateShard(Guid shardId, string type)
        {
            await _shardRepository.AddShardMetadataAsync(new Domain.Models.ShardMetadata()
            {
                ShardId = shardId,
                Type = type
            });
        }

        public List<AllocationCandidate> GetAllocationCandidates(Guid shardId, string type)
        {
            //Get all nodes that are contactable
            var activeNodes = _stateMachine.GetNodes().Where(node => node.IsContactable);

            ShardAllocationMetadata shard = _stateMachine.GetShard(type, shardId);
            List<AllocationCandidate> nodes = new List<AllocationCandidate>();
            foreach (var activeNode in activeNodes)
            {
                //If it is neither stale or insync, allocate the node
                if (!shard.InsyncAllocations.Contains(activeNode.Id) && !shard.StaleAllocations.Contains(activeNode.Id))
                {
                    nodes.Add(new AllocationCandidate()
                    {
                        NodeId = activeNode.Id,
                        Type = shard.Type
                    });
                }
            }
            return nodes;
        }
    }
}
