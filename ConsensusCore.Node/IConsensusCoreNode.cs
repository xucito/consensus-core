using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Node.Connectors;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace ConsensusCore.Node
{
    public interface IConsensusCoreNode<State>
        where State : BaseState, new()
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
        NodeInfo NodeInfo { get; }
        State GetState();
        Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new();
        bool InCluster { get; }
        bool IsLeader { get; }
        bool HasEntryBeenCommitted(int logIndex);
        List<BaseTask> GetClusterTasks();
        SortedList<int, LogEntry> GetLogs();
        //Used for testing
        void SetNodeRole(NodeState newState);
        List<ObjectLock> ObjectLocks { get; }
    }
}