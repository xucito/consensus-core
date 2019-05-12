using ConsensusCore.Clients;
using ConsensusCore.ViewModels;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.ValueObjects
{
    public class NodeConnection
    {
        public string Url { get; set; }
        public int KeepAliveIntervalMs { get; set; }
        public DateTime LastSuccessfulPoll { get; set; }
        Thread KeepAliveThread { get; set; }
        public bool IsConnectionAlive { get; set; }
        public NodeInfo NodeInfo { get; set; }
        public ILogger<NodeConnection> Logger { get; set; }
        public bool IsMe { get; set; }

        public NodeConnection(ILogger<NodeConnection> logger)
        {
            Logger = logger;
            LastSuccessfulPoll = DateTime.UtcNow;
        }

        public void Startup()
        {
            KeepAliveThread = new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                KeepAlive(KeepAliveIntervalMs);
            });
            if (!IsMe)
            {
                KeepAliveThread.Start();
            }
        }

        public void KeepAlive(int keepAliveIntervalMs)
        {
            var keepConnectionAlive = true;

            while (keepConnectionAlive)
            {
                int test= (DateTime.Now - LastSuccessfulPoll).Milliseconds;
                if (keepAliveIntervalMs < (int)(DateTime.UtcNow - LastSuccessfulPoll).TotalMilliseconds)
                {
                    try
                    {
                        NodeInfo = ConsensusCoreNodeClient.GetNodeInfoAsync(Url).GetAwaiter().GetResult();
                        RegisterCommunication();
                        Logger.LogDebug("Connection to " + NodeInfo.Id + " was successful.");
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Lost connection to " + NodeInfo.Id + "...");
                        IsConnectionAlive = false;
                    }
                }
                Thread.Sleep(keepAliveIntervalMs);
            }
        }

        public void RegisterCommunication()
        {
            if (!IsConnectionAlive)
            {
                Logger.LogInformation("Restored Connection to " + NodeInfo.Id + ".");
                IsConnectionAlive = true;
            }
            LastSuccessfulPoll = DateTime.UtcNow;
            IsConnectionAlive = true;
        }
    }
}
