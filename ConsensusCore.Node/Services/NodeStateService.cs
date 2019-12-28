using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services.Raft
{
    public class NodeStateService
    {
        public int CommitIndex { get; set; }
        public NodeState Role { get; private set; }
        public Guid Id { get; set; }
        public Dictionary<Guid, int> NextIndex { get; private set; } = new Dictionary<Guid, int>();
        public ConcurrentDictionary<Guid, int> MatchIndex { get; private set; } = new ConcurrentDictionary<Guid, int>();
        public int LatestLeaderCommit { get; set; }
        public string Url { get; set; }
        public bool IsBootstrapped { get; set; }
        public Guid? CurrentLeader { get; set; }
        public bool InCluster { get; set; }
        public NodeStatus Status
        {
            get
            {
                if (!InCluster)
                {
                    return NodeStatus.Red;
                }

                if (CommitIndex < LatestLeaderCommit)
                {
                    return NodeStatus.Yellow;
                }

                //Node is in cluster and all shards are synced
                return NodeStatus.Green;
            }
        }

        public void ResetLeaderState()
        {
            NextIndex.Clear();
            MatchIndex.Clear();
        }

        public string GetNodeLogId()
        {
            return "Node:" + Id + "(" + Url + "): ";
        }

        public bool SetRole(NodeState role)
        {
            Role = role;
            return true;
        }
    }

}
