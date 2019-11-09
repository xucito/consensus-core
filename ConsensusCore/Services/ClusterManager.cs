using ConsensusCore.Clients;
using ConsensusCore.Enums;
using ConsensusCore.Options;
using ConsensusCore.ValueObjects;
using ConsensusCore.ViewModels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Services
{
    /*
    public class ClusterManager
    {
        private ClusterOptions _options { get; set; }
        public ILoggerFactory LoggerFactory { get; set; }
        public ILogger<ClusterManager> Logger { get; set; }
        public Thread BootstrapNodeThread { get; set; }

        public Guid VotedFor { get; set; }
        public Guid CurrentLeader { get; set; }
        public bool IsInElection { get; set; }

        /// <summary>
        /// Node Id, address
        /// </summary>
        public Dictionary<Guid, NodeConnection> Connections { get; set; }
        private INodeManager _nodeManager { get; set; }

        public ClusterManager(IOptions<ClusterOptions> options, ILoggerFactory loggerFactory, INodeManager nodeManager)
        {
            _options = options.Value;

            LoggerFactory = loggerFactory;
            Logger = loggerFactory.CreateLogger<ClusterManager>();


            Connections = new Dictionary<Guid, NodeConnection>();

            _nodeManager = nodeManager;

            ConsensusCoreNodeClient.SetTimeout(TimeSpan.FromMilliseconds(_options.LatencyToleranceMs));

            // Always boot up as a follower
            _nodeManager.CurrentRole = NodeRole.Follower;

            BootstrapNodeThread = new Thread(() =>
            {
                Thread.Sleep(2000);
                Thread.CurrentThread.IsBackground = true;
                BootstrapNode();
            });
            BootstrapNodeThread.Start();
        }

        public void RegisterNodeRequest(Guid nodeId)
        {
            Connections[nodeId].RegisterCommunication();
        }

        public async void BootstrapNode()
        {
            List<string> bootstrapedUrls = new List<string>();

            while (_options.NodeUrls.Where(url => !bootstrapedUrls.Contains(url)).Count() > 0)
            {
                foreach (var url in _options.NodeUrls.Where(url => !bootstrapedUrls.Contains(url)))
                {
                    try
                    {
                        var node = await ConsensusCoreNodeClient.GetNodeInfoAsync(url);
                        var newNodeConnection = new NodeConnection(LoggerFactory.CreateLogger<NodeConnection>())
                        {
                            NodeInfo = node,
                            Url = url,
                            KeepAliveIntervalMs = _options.KeepAliveIntervalMs,
                            IsMe = node.Id == _nodeManager.Information.Id
                        };

                        Connections.Add(node.Id, newNodeConnection);
                        newNodeConnection.Startup();
                        bootstrapedUrls.Add(url);
                        Logger.LogInformation("Successfully registered " + url + " during bootstrapping.");
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Failed to establish connection to " + url + " during bootstrapping with error " + e.Message);
                    }
                }
            }
        }
    }*/
}
