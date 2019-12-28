using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Node.Communication.Clients;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Raft
{
    public class DiscoveryService<State> : BaseService<State> where State : BaseState, new()
    {
        DateTime lastWarningTime;
        ClusterConnectionPool _clusterConnectionPool;

        public DiscoveryService(ILogger<DiscoveryService<State>> logger, ClusterConnectionPool clusterConnectionPool, ClusterOptions clusterOptions, NodeOptions nodeOptions, IStateMachine<State> stateMachine,
            NodeStateService nodeStateService) : base(logger, clusterOptions, nodeOptions, stateMachine, nodeStateService)
        {
            _clusterConnectionPool = clusterConnectionPool;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task<List<BaseCommand>> DiscoverNodes()
        {
            var nodeUpsertCommands = new List<BaseCommand>();
            if (!ClusterOptions.TestMode)
            {
                Logger.LogDebug("Rediscovering nodes...");
                ConcurrentBag<Guid> NodesToMarkAsStale = new ConcurrentBag<Guid>();
                ConcurrentBag<Guid> NodesToRemove = new ConcurrentBag<Guid>();
                // Do not contact yourself
                var nodeUrlTasks = ClusterOptions.GetClusterUrls().Select(async url =>
                {
                    try
                    {
                        Guid? nodeId = null;
                        nodeId = (await new HttpNodeConnector(url, TimeSpan.FromMilliseconds(ClusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(ClusterOptions.DataTransferTimeoutMs)).GetNodeInfoAsync()).Id;

                        var possibleNodeUpdate = new NodeInformation()
                        {
                            Name = "",
                            TransportAddress = url
                        };

                        //If the node does not exist
                        if ((nodeId.Value != null && (!StateMachine.CurrentState.Nodes.ContainsKey(nodeId.Value) ||
                        // Check whether the node with the same id has different information
                        !StateMachine.CurrentState.Nodes[nodeId.Value].Equals(possibleNodeUpdate)
                        //Node was uncontactable now its contactable
                        || !StateMachine.IsNodeContactable(nodeId.Value))
                        ))
                        {
                            Logger.LogDebug("Detected updated for node " + nodeId);
                            nodeUpsertCommands.Add((BaseCommand)new UpsertNodeInformation()
                            {
                                Id = nodeId.Value,
                                Name = "",
                                TransportAddress = url,
                                IsContactable = true
                            });

                            var conflictingNodes = StateMachine.CurrentState.Nodes.Where(v => v.Value.TransportAddress == url && v.Key != nodeId);
                            // If there is another current node with that transport address
                            if (conflictingNodes.Count() > 0)
                            {
                                var conflictingNodeId = conflictingNodes.First().Key;
                                Logger.LogWarning("Detected another node with conflicting transport address, removing the conflicting node from the cluster");
                                nodeUpsertCommands.Add(new DeleteNodeInformation()
                                {
                                    Id = conflictingNodes.First().Key
                                });
                                NodesToRemove.Add(conflictingNodeId);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Guid? nodeAtUrl;
                        if ((nodeAtUrl = _clusterConnectionPool.NodeAtUrl(url)) != null && StateMachine.IsNodeContactable(nodeAtUrl.Value))
                            Logger.LogWarning("Node at url " + url + " became unreachable...");
                        var staleNode = StateMachine.GetNode(url);
                        if (staleNode != null)
                            NodesToMarkAsStale.Add(staleNode.Id);
                    }
                });

                await Task.WhenAll(nodeUrlTasks);
                var rand = new Random();

                if ((NodesToMarkAsStale.Count() > 0 || NodesToRemove.Count() > 0) && ClusterOptions.MinimumNodes > 1)
                {
                    if ((lastWarningTime - DateTime.Now).TotalMilliseconds > 3000)
                    {
                        lastWarningTime = DateTime.Now;
                        Logger.LogWarning("Found stale or removed nodes, reassigning all nodes");
                    }

                    foreach (var shard in StateMachine.GetShards())
                    {
                        if (NodesToMarkAsStale.Contains(shard.PrimaryAllocation) || shard.InsyncAllocations.Where(i => NodesToRemove.Contains(i) || NodesToMarkAsStale.Contains(i)).Count() > 0 || shard.StaleAllocations.Where(i => NodesToRemove.Contains(i)).Count() > 0)
                        {
                            Logger.LogDebug("Reassigned shard " + shard.Id);
                            var invalidInsyncAllocations = shard.InsyncAllocations.Where(ia => NodesToRemove.Contains(ia) || NodesToMarkAsStale.Contains(ia));
                            //Get node with the highest shard
                            var insyncAllocations = shard.InsyncAllocations.Where(ia => !invalidInsyncAllocations.Contains(ia));
                            if (insyncAllocations.Count() > 0)
                            {
                                Guid newPrimary = !invalidInsyncAllocations.Contains(shard.PrimaryAllocation) ? shard.PrimaryAllocation : insyncAllocations.ElementAt(rand.Next(0, insyncAllocations.Count()));// await GetMostUpdatedNode(shard.Id, shard.Type, shard.InsyncAllocations.Where(ia => !invalidInsyncAllocations.Contains(ia)).ToArray());
                                nodeUpsertCommands.Add(new UpdateShardMetadataAllocations()
                                {
                                    PrimaryAllocation = newPrimary,
                                    InsyncAllocationsToRemove = invalidInsyncAllocations.ToHashSet<Guid>(),
                                    StaleAllocationsToAdd = NodesToMarkAsStale.ToHashSet<Guid>(),
                                    StaleAllocationsToRemove = NodesToRemove.ToHashSet<Guid>(),
                                    ShardId = shard.Id,
                                    Type = shard.Type,
                                    DebugLog = "Reassigning nodes based on node becoming unavailable. Primary node is " + newPrimary
                                });
                            }
                            else
                            {
                                Logger.LogError("Shard " + shard.Id + " does not have an assignable in-sync primary allocation. Awaiting for shard " + shard.PrimaryAllocation + " to come back online...");
                            }
                        }
                    }
                }

                foreach (var nodeId in NodesToMarkAsStale)
                {
                    //There could be already requests in queue marking the node as unreachable
                    if (StateMachine.IsNodeContactable(nodeId))
                    {
                        nodeUpsertCommands.Add(new UpsertNodeInformation()
                        {
                            IsContactable = false,
                            Id = nodeId,
                            TransportAddress = StateMachine.GetNodes().Where(n => n.Id == nodeId).First().TransportAddress,
                            DebugLog = "Node became unreachable"
                        });
                    }
                }
            }
            else
            {
                //In the test, just add your own node
                nodeUpsertCommands.Add((BaseCommand)new UpsertNodeInformation()
                {
                    Id = NodeStateService.Id,
                    Name = "",
                    IsContactable = true
                });
            }

            //Check object locks
            var objectLocks = StateMachine.GetObjectLocks();

            foreach (var objectLock in objectLocks)
            {
                if (objectLock.Value.IsExpired)
                {
                    Logger.LogWarning("Found expired lock on object " + objectLock.Key + ", releasing lock.");
                    nodeUpsertCommands.Add(new RemoveObjectLock()
                    {
                        ObjectId = objectLock.Value.ObjectId,
                        Type = objectLock.Value.Type
                    });
                }
            }
            return nodeUpsertCommands;
        }
    }
}
