using System;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;

namespace ConsensusCore.Node.Communication.Controllers
{
    public interface IClusterRequestHandler
    {
        Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new();
        Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<Task<TResponse>> Handle) where TResponse : BaseResponse, new();
        bool IsClusterRequest<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse;
    }
}