using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Connectors
{
    public interface INodeClient: IDisposable
    {
        Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse;
        Task<NodeInfo> GetNodeInfoAsync();
        string Address { get; }
    }
}
