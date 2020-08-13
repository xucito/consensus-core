using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Node.Communication;
using ConsensusCore.Node.Communication.Clients;
using ConsensusCore.Node.Communication.Controllers;
using ConsensusCore.Node.Connectors.Exceptions;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Connectors
{
    public class ClusterClient
    {
        private IClusterConnectionPool _clusterConnectionPool { get; set; }
        private IServiceProvider _serviceProvider;
        private IClusterRequestHandler _clusterRequestHandler { get { return (IClusterRequestHandler)_serviceProvider.GetService(typeof(IClusterRequestHandler)); } }
        private NodeStateService _nodeStateService;

        public ClusterClient(
            IClusterConnectionPool connectionPool,
            IServiceProvider serviceProvider,
            NodeStateService nodeStateService
           )
        {
            _clusterConnectionPool = connectionPool;
            _nodeStateService = nodeStateService;
            _serviceProvider = serviceProvider;
        }

        public async Task<TResponse> Send<TResponse>(Guid nodeId, IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {
            //if the request is for yourself
            if (_nodeStateService.Id == nodeId)
            {
                return await Send(request);
            }
            if (_clusterConnectionPool.ContainsNode(nodeId))
            {
                return await _clusterConnectionPool.GetNodeClient(nodeId).Send(request);
            }
            else
            {
                throw new MissingNodeConnectionDetailsException("Node " + nodeId + " was not found in available connections.");
            }
        }

        public async Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {
            return await _clusterRequestHandler.Handle(request);
        }
    }
}
