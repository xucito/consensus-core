using ConsensusCore.Node.Connectors;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsensusCore.Node.Communication.Clients
{
    public class ClusterConnectionPool
    {
        private Dictionary<Guid, INodeClient> _nodeConnectors = new Dictionary<Guid, INodeClient>();
        private Dictionary<Guid, INodeClient> NodeConnectors
        {
            get
            {
                return _nodeConnectors.ToDictionary(entry => entry.Key, entry => entry.Value);
            }
        }
        private TimeSpan _timeoutInterval { get; set; }
        private TimeSpan _dataTimeoutInterval { get; set; }
        public int TotalClients { get { return _nodeConnectors.Count(); } }

        object _connectorLock = new object();

        public ClusterConnectionPool(TimeSpan timeoutInterval, TimeSpan dataTimeoutInterval)
        {
            _timeoutInterval = timeoutInterval;
            _dataTimeoutInterval = dataTimeoutInterval;
        }

        public ClusterConnectionPool(IOptions<ClusterOptions> clusterOptions)
        {
            _timeoutInterval = TimeSpan.FromMilliseconds(clusterOptions.Value.CommitsTimeout);
            _dataTimeoutInterval = TimeSpan.FromMilliseconds(clusterOptions.Value.DataTransferTimeoutMs);
        }

        public void Reset()
        {
            _nodeConnectors = new Dictionary<Guid, INodeClient>();
        }

        public void AddClient(Guid nodeId, string url)
        {
            lock (_connectorLock)
            {
                Guid existingNode;
                //If the key exists
                if ((existingNode = _nodeConnectors.Where(nc => nc.Value.Address == url).Select(k => k.Key).FirstOrDefault()) != default(Guid))
                {
                    _nodeConnectors.Remove(existingNode);
                }

                _nodeConnectors.Add(nodeId, new HttpNodeConnector(url, _timeoutInterval, _dataTimeoutInterval));
            }
        }

        /// <summary>
        /// Returns whether the node has a connector
        /// </summary>
        /// <param name="nodeId"></param>
        /// <returns></returns>
        public bool ContainsNode(Guid nodeId)
        {
            return _nodeConnectors.ContainsKey(nodeId);
        }

        public Guid? NodeAtUrl(string url)
        {
            var clients = _nodeConnectors.Where(nc => nc.Value.Address == url);
            if(clients.Count() > 0)
            {
                return clients.First().Key;
            }
            return null;
        }

        public INodeClient GetNodeClient(Guid nodeId)
        {
            return _nodeConnectors[nodeId];
        }

        public string GetNodeClientAddress(Guid nodeId)
        {
            return _nodeConnectors[nodeId].Address;
        }

        public Dictionary<Guid, INodeClient> GetAllNodeClients()
        {
            return NodeConnectors;
        }
    }
}
