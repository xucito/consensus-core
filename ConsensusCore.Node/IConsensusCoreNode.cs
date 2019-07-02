using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Node.Connectors;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace ConsensusCore.Node
{
    public interface IConsensusCoreNode<State, Repository>
        where State : BaseState, new()
        where Repository : IBaseRepository
    {
        /* StateMachine<State> _stateMachine { get; }
         int CommitIndex { get; }
         NodeState CurrentState { get; }
         ILogger<ConsensusCoreNode<State, Repository>> Logger { get; }
         string MyUrl { get; }
         Dictionary<string, HttpNodeConnector> NodeConnectors { get; }
         NodeInfo NodeInfo { get; }
         Dictionary<string, int> NextIndex { get; }
         Dictionary<string, int> MatchIndex { get; }

         int ProcessCommandsRequestHandler(ProcessCommandsRequest request);
         bool AppendEntry(AppendEntry entry);
         void BootstrapNode();
         void ElectionTimeoutEventHandler(object args);
         State GetState();
         void HeartbeatTimeoutEventHandler(object args);
         bool RequestVote(RequestVote requestVoteRPC);
         void ResetLeaderState();
         void SendHeartbeats();
         void SetNodeRole(NodeState newState);
         bool AssignDataShard(AssignDataShard shard);
         Guid? CreateNewShardRequestHandler(CreateDataShardRequest shard);
         bool UpdateShardCommand(Guid id,string type, object newData);
         object GetData(Guid id, string type);*/
        Dictionary<Guid, LocalShardMetaData> LocalShards { get; }
        NodeInfo NodeInfo { get; }
        State GetState();
        Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request);
        bool InCluster { get; }
        bool IsLeader { get; }
    }
}