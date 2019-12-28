using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Communication.Clients;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services.Raft
{
    public class SnapshotService<State> where State : BaseState, new()
    {
        INodeStorage<State> _nodeStorage;
        ILogger _logger;
        IStateMachine<State> _stateMachine;
        NodeStateService _nodeStateService;

        public SnapshotService(ILogger<SnapshotService<State>> logger,
            INodeStorage<State> nodeStorage,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService)
        {
            _logger = logger;
            _stateMachine = stateMachine;
            _nodeStorage = nodeStorage;
            _nodeStateService = nodeStateService;
        }


        public InstallSnapshotResponse InstallSnapshotHandler(InstallSnapshot request)
        {
            if (request.Term < _nodeStorage.CurrentTerm)
            {
                return new InstallSnapshotResponse()
                {
                    IsSuccessful = false,
                    Term = request.Term
                };
            }

            if (_nodeStorage.GetTotalLogCount() < request.LastIncludedIndex)
            {
                _nodeStorage.SetLastSnapshot(((JObject)request.Snapshot).ToObject<State>(), request.LastIncludedIndex, request.LastIncludedTerm);
                _stateMachine.ApplySnapshotToStateMachine(((JObject)request.Snapshot).ToObject<State>());
                _nodeStateService.CommitIndex = request.LastIncludedIndex;
                _nodeStorage.SetCurrentTerm(request.LastIncludedTerm);
                return new InstallSnapshotResponse()
                {
                    IsSuccessful = true,
                    Term = request.Term
                };
            }
            else
            {
                return new InstallSnapshotResponse()
                {
                    IsSuccessful = true,
                    Term = request.Term
                };
            }
        }
    }
}
