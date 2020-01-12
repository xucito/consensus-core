using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Communication.Controllers
{
    public class ClusterRequestHandler<State> : IClusterRequestHandler where State : BaseState, new()
    {
        private readonly ILogger _logger;
        private readonly IRaftService _raftService;
        private readonly NodeStateService _nodeStateService;
        private readonly ClusterOptions _clusterOptions;
        private readonly ClusterClient _clusterClient;
        private readonly IDataService _dataService;
        public event EventHandler<Metric> MetricGenerated;
        public ConcurrentDictionary<string, DateTime> lastMetricGenerated = new ConcurrentDictionary<string, DateTime>(); 

        public ClusterRequestHandler(
            IOptions<ClusterOptions> clusterOptions,
            ILogger<ClusterRequestHandler<State>> logger,
            IRaftService raftService,
            NodeStateService nodeStateService,
            ClusterClient clusterClient,
            IDataService dataService)
        {
            _clusterClient = clusterClient;
            _clusterOptions = clusterOptions.Value;
            _nodeStateService = nodeStateService;
            _logger = logger;
            _raftService = raftService;
            _dataService = dataService;
        }

        public async Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
        {

            _logger.LogDebug(_nodeStateService.GetNodeLogId() + "Detected RPC " + request.GetType().Name + "." + Environment.NewLine + JsonConvert.SerializeObject(request, Formatting.Indented));
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

            DateTime commandStartTime = DateTime.Now;

            try
            {
                TResponse response;
                switch (request)
                {
                    case ExecuteCommands t1:
                        response = await HandleIfLeaderOrReroute(request, async () => (TResponse)(object)await _raftService.Handle(t1));
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
                    case RequestCreateIndex t1:
                        response = await HandleIfLeaderOrReroute(request, async () => (TResponse)(object)await _dataService.Handle(t1));
                        break;
                    case AddShardWriteOperation t1:
                        response = (TResponse)(object)await _dataService.Handle(t1);
                        break;
                    case RequestDataShard t1:
                        response = (TResponse)(object)await _dataService.Handle(t1);
                        break;
                    case AllocateShard t1:
                        response = (TResponse)(object)await _dataService.Handle(t1);
                        break;
                    case ReplicateShardWriteOperation t1:
                        response = (TResponse)(object)await _dataService.Handle(t1);
                        break;
                    case RequestShardWriteOperations t1:
                        response = (TResponse)(object)await _dataService.Handle(t1);
                        break;
                    default:
                        throw new Exception("Request is not implemented");
                }

                if (MetricGenerated != null && _nodeStateService.Role == NodeState.Leader && request.Metric)
                {
                    //Add and send
                    if(!lastMetricGenerated.ContainsKey(request.RequestName))
                    {
                        lastMetricGenerated.TryAdd(request.RequestName, DateTime.Now);
                        MetricGenerated.Invoke(this, new Metric()
                        {
                            Date = DateTime.Now,
                            IntervalMs = 0,
                            Type = MetricTypes.ClusterCommandElapsed(request.RequestName),
                            Value = (DateTime.Now - commandStartTime).TotalMilliseconds
                        });
                    }
                    else if((DateTime.Now - lastMetricGenerated[request.RequestName]).TotalMilliseconds > _clusterOptions.MetricsIntervalMs)
                    {
                        lastMetricGenerated.TryUpdate(request.RequestName, DateTime.Now, lastMetricGenerated[request.RequestName]);
                        MetricGenerated.Invoke(this, new Metric()
                        {
                            Date = DateTime.Now,
                            IntervalMs = 0,
                            Type = MetricTypes.ClusterCommandElapsed(request.RequestName),
                            Value = (DateTime.Now - commandStartTime).TotalMilliseconds
                        });
                    }
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

        public async Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<Task<TResponse>> Handle) where TResponse : BaseResponse, new()
        {
            var CurrentTime = DateTime.Now;
            // if you change and become a leader, just handle this yourself.
            if (_nodeStateService.Role != NodeState.Leader)
            {
                // Candidate, not in cluster or you still think current leader is yourself.
                if (_nodeStateService.Role == NodeState.Candidate || !_nodeStateService.InCluster || _nodeStateService.Id == _nodeStateService.CurrentLeader.Value)
                {
                    /* if ((DateTime.Now - CurrentTime).TotalMilliseconds < _clusterOptions.LatencyToleranceMs)
                     {
                         _logger.LogWarning(_nodeStateService.GetNodeLogId() + "Currently a candidate during routing, will sleep thread and try again.");
                         Thread.Sleep(1000);
                     }
                     else
                     {*/
                    return new TResponse()
                    {
                        IsSuccessful = false
                    };
                    //}
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
            return await Handle();
        }



        public bool IsClusterRequest<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse
        {
            switch (request)
            {
                case ExecuteCommands t1:
                    return true;
                case RequestCreateIndex t1:
                    return true;
                case AddShardWriteOperation t1:
                    return true;
                case RequestDataShard t1:
                    return true;
                case AllocateShard t1:
                    return true;
                case ReplicateShardWriteOperation t1:
                    return true;
                case RequestShardWriteOperations t1:
                    return true;
                default:
                    return false;
            }
        }
    }
}
