using System;
using System.Collections.Generic;
using ConsensusCore.Node.Connectors;

namespace ConsensusCore.Node.Communication.Clients
{
    public interface IClusterConnectionPool
    {
        int TotalClients { get; }

        bool ContainsNode(Guid nodeId);
        Dictionary<Guid, INodeClient> GetAllNodeClients();
        INodeClient GetNodeClient(Guid nodeId);
        string GetNodeClientAddress(Guid nodeId);
        Guid? NodeAtUrl(string url);
        void AddClient(Guid nodeId, string url);
        void RemoveClient(Guid nodeId);
        void CheckClusterConnectionPool();
    }
}