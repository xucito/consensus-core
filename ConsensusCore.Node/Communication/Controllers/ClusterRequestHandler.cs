using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Communication.Controllers
{
    public class ClusterRequestHandler<State> where State : BaseState, new()
    {
        ILogger _logger;
        IRaftService
            _raftService;
        public ClusterRequestHandler(ILogger<ClusterRequestHandler<State>> logger, IRaftService raftService)
        {
            _logger = logger;
            _raftService = raftService;
        }

        public async Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new()
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
    }
}
