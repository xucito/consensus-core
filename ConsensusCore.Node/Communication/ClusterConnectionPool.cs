using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Node.Connectors;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsensusCore.Node.Communication.Clients
{
    public class ClusterConnectionPool<State> : IClusterConnectionPool where State : BaseState, new()
    {
        private Dictionary<Guid, INodeClient> _nodeConnectors = new Dictionary<Guid, INodeClient>();
        private Dictionary<Guid, INodeClient> NodeConnectors
        {
            get
            {
                lock (_connectorLock)
                {
                    return _nodeConnectors.ToDictionary(entry => entry.Key, entry => entry.Value);
                }
            }
        }
        private TimeSpan _timeoutInterval { get; set; }
        private TimeSpan _dataTimeoutInterval { get; set; }
        IStateMachine<State> _stateMachine { get; set; }
        public int TotalClients { get { return _nodeConnectors.Count(); } }

        object _connectorLock = new object();

        public ClusterConnectionPool(IStateMachine<State> stateMachine, TimeSpan timeoutInterval, TimeSpan dataTimeoutInterval)
        {
            _timeoutInterval = timeoutInterval;
            _dataTimeoutInterval = dataTimeoutInterval;
            _stateMachine = stateMachine;
        }

        public ClusterConnectionPool(IStateMachine<State> stateMachine, IOptions<ClusterOptions> clusterOptions)
        {
            _stateMachine = stateMachine;
            _timeoutInterval = TimeSpan.FromMilliseconds(clusterOptions.Value.CommitsTimeout);
            _dataTimeoutInterval = TimeSpan.FromMilliseconds(clusterOptions.Value.DataTransferTimeoutMs);
        }

        public void Reset()
        {
            lock (_connectorLock)
            {
                _nodeConnectors = new Dictionary<Guid, INodeClient>();
            }
        }

        public void AddClient(Guid nodeId, string url)
        {
            lock (_connectorLock)
            {
                Guid existingNode;
                //If the key exists
                var matchingExistingNodes = _nodeConnectors.Where(nc => nc.Value.Address == url).Select(a => a.Key).ToList();
                foreach (var matchingNode in matchingExistingNodes)
                {
                    _nodeConnectors.Remove(matchingNode);
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
           if (!_nodeConnectors.ContainsKey(nodeId))
            {
                CheckAndAddFromState(nodeId);
            }
            return _nodeConnectors.ContainsKey(nodeId);
        }

        public Guid? NodeAtUrl(string url)
        {
            lock (_connectorLock)
            {
                var clients = _nodeConnectors.Where(nc => nc.Value.Address == url);
                if (clients.Count() > 0)
                {
                    return clients.First().Key;
                }
                return null;
            }
        }

        public INodeClient GetNodeClient(Guid nodeId)
        {
            CheckAndAddFromState(nodeId);
            return _nodeConnectors[nodeId];
        }

        public string GetNodeClientAddress(Guid nodeId)
        {
            CheckAndAddFromState(nodeId);
            return _nodeConnectors[nodeId].Address;
        }

        public Dictionary<Guid, INodeClient> GetAllNodeClients()
        {
            return NodeConnectors;
        }

        public void RemoveClient(Guid nodeId)
        {
            lock (_connectorLock)
            {
                _nodeConnectors.Remove(nodeId);
            }
        }

        private void CheckAndAddFromState(Guid nodeId)
        {
            if (!_nodeConnectors.ContainsKey(nodeId))
            {
                var node = _stateMachine.GetNode(nodeId);
                if (node != null)
                {
                    AddClient(nodeId, node.TransportAddress);
                }
            }
        }

        public void CheckClusterConnectionPool()
        {
            if (_stateMachine != null)
            {
                var nodes = _stateMachine.GetNodes();
                foreach (var node in nodes)
                {
                    if (!_nodeConnectors.ContainsKey(node.Id))
                    {
                        lock (_nodeConnectors)
                        {
                            AddClient(node.Id, node.TransportAddress);
                        }
                    }
                }
            }
        }
    }
}
