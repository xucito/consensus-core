using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Node.Communication;
using ConsensusCore.Node.Communication.Clients;
using ConsensusCore.Node.Connectors.Exceptions;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Connectors
{
    public class ClusterClient
    {
        private ClusterConnectionPool _clusterConnectionPool { get; set; }

        public ClusterClient(ClusterConnectionPool connectionPool)
        {
            _clusterConnectionPool = connectionPool;
        }

        public async Task<TResponse> Send<TResponse>(Guid nodeId, IClusterRequest<TResponse> request) where TResponse : BaseResponse
        {
            if(_clusterConnectionPool.ContainsNode(nodeId))
            {
                return await _clusterConnectionPool.GetNodeClient(nodeId).Send(request);
            }
            else
            {
                throw new MissingNodeConnectionDetailsException("Node " + nodeId + " was not found in available connections.");
            }
        }
    }
}
