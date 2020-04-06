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
    public class Bootstrapper<State> where State : BaseState, new()
    {
        INodeStorage<State> _nodeStorage;
        ILogger Logger;

        public Bootstrapper(ILogger<Bootstrapper<State>> logger,
            ClusterOptions clusterOptions,
            NodeOptions nodeOptions,
            INodeStorage<State> nodeStorage,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService)
        {
            _nodeStorage = nodeStorage;
            Logger = logger;

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

        public async Task<string> GetMyUrl(IEnumerable<string> urls, TimeSpan latencyMs)
        {
            string myUrl = null;
            while (myUrl == null)
            {
                foreach (var url in urls)
                {
                    //Create a test controller temporarily
                    var testConnector = new HttpNodeConnector(url, latencyMs, TimeSpan.FromMilliseconds(-1));

                    Guid? nodeId = null;
                    try
                    {
                        nodeId = (await testConnector.GetNodeInfoAsync()).Id;
                        if (nodeId == _nodeStorage.Id)
                        {
                            myUrl = url;
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
            }
            return myUrl;
        }
    }
}
