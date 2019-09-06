using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Node.Connectors.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Connectors
{
    public class ClusterConnector
    {
        private TimeSpan _timeoutInterval { get; set; }
        private TimeSpan _dataTimeoutInterval { get; set; }
        public int TotalNodes { get { return _nodeConnectors.Count(); } }

        object _connectorLock = new object();
        private Dictionary<Guid, HttpNodeConnector> _nodeConnectors = new Dictionary<Guid, HttpNodeConnector>();
        public Dictionary<Guid, HttpNodeConnector> NodeConnectors { get { return _nodeConnectors.ToDictionary(entry => entry.Key,
                                               entry => entry.Value);
            } }

        public ClusterConnector(TimeSpan timeoutInterval, TimeSpan dataTimeoutInterval)
        {
            _timeoutInterval = timeoutInterval;
            _dataTimeoutInterval = dataTimeoutInterval;
        }

        public async Task<TResponse> Send<TResponse>(Guid nodeId, IClusterRequest<TResponse> request) where TResponse : BaseResponse
        {
            if(_nodeConnectors.ContainsKey(nodeId))
            {
                return await _nodeConnectors[nodeId].Send(request);
            }
            else
            {
                throw new MissingNodeConnectionDetailsException("Node " + nodeId + " was not found in available connections.");
            }
        }

        public void ClearConnector()
        {
            _nodeConnectors = new Dictionary<Guid, HttpNodeConnector>();
        }

        public void AddConnector(Guid nodeId, string url)
        {
            lock(_connectorLock)
            {
                Guid existingNode;
                //If the key exists
                if((existingNode = _nodeConnectors.Where(nc => nc.Value.Url == url).Select(k => k.Key).FirstOrDefault()) != default(Guid))
                {
                    _nodeConnectors.Remove(existingNode);
                }

                _nodeConnectors.Add(nodeId, new HttpNodeConnector(url, _timeoutInterval, _dataTimeoutInterval));
            }
        }

        public string GetNodeUrl(Guid nodeId)
        {
            if(_nodeConnectors.ContainsKey(nodeId))
            {
                return _nodeConnectors[nodeId].Url;
            }
            return null;
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
    }
}
