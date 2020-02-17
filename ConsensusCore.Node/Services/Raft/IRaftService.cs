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
        InstallSnapshotResponse InstallSnapshotHandler(InstallSnapshot request);
        Task MonitorCommits(bool loop);
        RequestVoteResponse RequestVoteRPCHandler(RequestVote requestVoteRPC);
        void SendHeartbeats();
        void SetNodeRole(NodeState newState);
        void StartElection();
    }
}