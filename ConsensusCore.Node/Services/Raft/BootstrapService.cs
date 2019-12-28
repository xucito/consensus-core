using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Communication.Clients;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Raft
{
    public class BootstrapService<State> : BaseService<State> where State : BaseState, new()
    {
        ClusterConnectionPool _clusterConnectionPool;
        INodeStorage<State> _nodeStorage;

        public BootstrapService(ILogger<BootstrapService<State>> logger,
            ClusterConnectionPool clusterConnectionPool, 
            ClusterOptions clusterOptions, NodeOptions nodeOptions, INodeStorage<State> nodeStorage, IStateMachine<State> stateMachine,
            NodeStateService nodeStateService) : base(logger, clusterOptions, nodeOptions, stateMachine, nodeStateService)
        {
            _clusterConnectionPool = clusterConnectionPool;
            _nodeStorage = nodeStorage;

            //Load the last snapshot
            if (_nodeStorage.LastSnapshot != null)
            {
                Logger.LogInformation("Detected snapshot, loading snapshot into state.");
                stateMachine.ApplySnapshotToStateMachine(_nodeStorage.LastSnapshot);
                nodeStateService.CommitIndex = _nodeStorage.LastSnapshotIncludedIndex;
                _nodeStorage.SetCurrentTerm(_nodeStorage.LastSnapshotIncludedTerm);
                nodeStateService.Id = _nodeStorage.Id;
            }
        }

        public async Task<bool> BootstrapNode()
        {
            Logger.LogInformation("Bootstrapping Node!");

            // The node cannot bootstrap unless at least a majority of nodes are present
            _clusterConnectionPool.Reset();
            string myUrl = null;
            while (myUrl == null)
            {
                foreach (var url in ClusterOptions.NodeUrls.Split(','))
                {
                    //Create a test controller temporarily
                    var testConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(ClusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(ClusterOptions.DataTransferTimeoutMs));

                    Guid? nodeId = null;
                    try
                    {
                        nodeId = (await testConnector.GetNodeInfoAsync()).Id;
                        if (nodeId == _nodeStorage.Id)
                        {
                            myUrl = url;
                        }
                        else
                        {
                            _clusterConnectionPool.AddClient(nodeId.Value, url);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Node at url " + url + " was unreachable...");
                    }
                }

                if (myUrl == null)
                {
                    Logger.LogWarning("Node is not discoverable from the given node urls!");
                }

                if (_clusterConnectionPool.TotalClients < ClusterOptions.MinimumNodes - 1)
                {
                    Logger.LogWarning("Not enough of the nodes in the cluster are contactable, awaiting bootstrap");
                }
            }
            return true;
        }
    }
}
