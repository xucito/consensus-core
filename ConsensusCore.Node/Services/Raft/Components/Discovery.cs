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
    public class Discovery
    {
        public ILogger<Discovery> Logger;

        public Discovery(ILogger<Discovery> logger)
        {
            Logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<NodeInformation>> SearchForNodes(IEnumerable<string> urls, TimeSpan latencyTolerance)
        {
            List<NodeInformation> newNodes = new List<NodeInformation> ();
            Logger.LogDebug("Rediscovering nodes...");
            var nodeUrlTasks = urls.Select(async url =>
            {
                try
                {
                    Guid? nodeId = null;
                    nodeId = (await new HttpNodeConnector(url, latencyTolerance, TimeSpan.FromMilliseconds(-1)).GetNodeInfoAsync()).Id;
                    //If the node does not exist
                    if (nodeId.Value != null)
                    {
                        Logger.LogDebug("Detected updated for node " + nodeId);
                        newNodes.Add(new NodeInformation()
                        {
                            Id = nodeId.Value,
                            IsContactable = true,
                            TransportAddress = url
                        });
                    }
                }
                catch (Exception e)
                {
                    Logger.LogWarning("Node at url " + url + " is still unreachable...");
                }
            });

            await Task.WhenAll(nodeUrlTasks);
            return newNodes;
        }
    }
}
