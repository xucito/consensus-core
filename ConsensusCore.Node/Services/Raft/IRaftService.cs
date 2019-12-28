using System;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.RPCs.Raft;

namespace ConsensusCore.Node.Services.Raft
{
    public interface IRaftService
    {
        AppendEntryResponse AppendEntryRPCHandler(AppendEntry entry);
        void ElectionTimeoutEventHandler(object args);
        ExecuteCommandsResponse ExecuteCommandsRPCHandler(ExecuteCommands request);
        Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new();
        Task<TResponse> HandleIfLeaderOrReroute<TResponse>(IClusterRequest<TResponse> request, Func<TResponse> Handle) where TResponse : BaseResponse, new();
        void HeartbeatTimeoutEventHandler(object args);
        InstallSnapshotResponse InstallSnapshotHandler(InstallSnapshot request);
        bool IsClusterRequest<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse;
        Task MonitorCommits();
        RequestVoteResponse RequestVoteRPCHandler(RequestVote requestVoteRPC);
        void SendHeartbeats();
        void SetNodeRole(NodeState newState);
        void StartElection();
    }
}