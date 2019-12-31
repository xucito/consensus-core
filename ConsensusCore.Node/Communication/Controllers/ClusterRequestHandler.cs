﻿using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Communication.Controllers
{
    public class ClusterRequestHandler<State> where State : BaseState, new()
    {
        private readonly ILogger _logger;
        private readonly IRaftService _raftService;
        private readonly NodeStateService _nodeStateService;
        private readonly ClusterOptions _clusterOptions;
        private readonly ClusterClient _clusterClient;

        public ClusterRequestHandler(
            IOptions<ClusterOptions> clusterOptions,
            ILogger<ClusterRequestHandler<State>> logger,
            IRaftService raftService,
            NodeStateService nodeStateService,
            ClusterClient clusterClient)
        {
            _clusterClient = clusterClient;
            _clusterOptions = clusterOptions.Value;
            _nodeStateService = nodeStateService;
            _logger = logger;
            _raftService = raftService;
        }

        public async Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {

            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Detected RPC " + request.GetType().Name + ".");
            if (!_nodeStateService.IsBootstrapped)
            {
                _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Node is not ready...");

                return new TResponse()
                {
                    IsSuccessful = false,
                    ErrorMessage = "Node is not ready..."
                };
            }

            if (IsClusterRequest<TResponse>(request) && !_nodeStateService.InCluster)
            {
                _logger.LogWarning(_nodeStateService.GetNodeLogId() + "Reqeuest rejected, node is not apart of cluster...");
                return new TResponse()
                {
                    IsSuccessful = false,
                    ErrorMessage = "Node is not apart of cluster..."
                };
            }

            try
            {
                TResponse response;
                switch (request)
                {
                    case ExecuteCommands t1:
                        response = (TResponse)(object)await _raftService.Handle(t1);
                        break;
                    case RequestVote t1:
                        response = (TResponse)(object)await _raftService.Handle(t1);
                        break;
                    case AppendEntry t1:
                        response = (TResponse)(object)await _raftService.Handle(t1);
                        break;
                    case InstallSnapshot t1:
                        response = (TResponse)(object)await _raftService.Handle(t1);
                        break;
                    default:
                        throw new Exception("Request is not implemented");
                }

                return response;

            }
            catch (TaskCanceledException e)
            {
                _logger.LogWarning(_nodeStateService.GetNodeLogId() + "Request " + request.RequestName + " timed out...");
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }
            catch (Exception e)
            {
                _logger.LogError(_nodeStateService.GetNodeLogId() + "Failed to handle request " + request.RequestName + " with error " + e.Message + Environment.StackTrace + e.StackTrace);
                return new TResponse()
                {
                    IsSuccessful = false
                };
            }

        }

        public async Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<TResponse> Handle) where TResponse : BaseResponse, new()
        {
            var CurrentTime = DateTime.Now;
            // if you change and become a leader, just handle this yourself.
            while (_nodeStateService.Role != NodeState.Leader)
            {
                if (_nodeStateService.Role == NodeState.Candidate)
                {
                    if ((DateTime.Now - CurrentTime).TotalMilliseconds < _clusterOptions.LatencyToleranceMs)
                    {
                        _logger.LogWarning(_nodeStateService.GetNodeLogId() + "Currently a candidate during routing, will sleep thread and try again.");
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        return new TResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                }
                else
                {
                    try
                    {
                        _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Detected routing of command " + request.GetType().Name + " to leader.");
                        return (TResponse)(object)await _clusterClient.Send(_nodeStateService.CurrentLeader.Value, request);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(_nodeStateService.GetNodeLogId() + "Encountered " + e.Message + " while trying to route " + request.GetType().Name + " to leader.");
                        return new TResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                }
            }
            return Handle();
        }



        public bool IsClusterRequest<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse
        {
            switch (request)
            {
                case ExecuteCommands t1:
                    return true;
                default:
                    return false;
            }
        }
    }
}
