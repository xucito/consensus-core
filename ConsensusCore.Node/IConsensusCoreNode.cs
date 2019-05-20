using System;
using System.Collections.Generic;
using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Messages;
using ConsensusCore.Node.Models;
using Microsoft.Extensions.Logging;

namespace ConsensusCore.Node
{
    public interface IConsensusCoreNode<Command, State, Repository>
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where Repository : BaseRepository<Command>
    {
        StateMachine<Command, State> _stateMachine { get; }
        int CommitIndex { get; }
        NodeState CurrentState { get; }
        ILogger<ConsensusCoreNode<Command, State, Repository>> Logger { get; }
        string MyUrl { get; }
        Dictionary<string, HttpNodeConnector> NodeConnectors { get; }
        NodeInfo NodeInfo { get; }
        Dictionary<string, int> NextIndex { get; }
        Dictionary<string, int> MatchIndex { get; }

        int AddCommand(List<Command> command, bool waitForCommit = false);
        bool AppendEntry(AppendEntry<Command> entry);
        void BootstrapNode();
        void ElectionTimeoutEventHandler(object args);
        State GetState();
        void HeartbeatTimeoutEventHandler(object args);
        bool RequestVote(RequestVote requestVoteRPC);
        void ResetLeaderState();
        void SendHeartbeats();
        void SetNodeRole(NodeState newState);
    }
}