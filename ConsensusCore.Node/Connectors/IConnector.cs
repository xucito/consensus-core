using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Connectors
{
    public interface IConnector
    {
        Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request);
        Task<NodeInfo> GetNodeInfoAsync();
    }
}
