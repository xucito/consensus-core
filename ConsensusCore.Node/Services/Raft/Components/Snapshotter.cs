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
    public class Snapshotter<State> where State : BaseState, new()
    {
        INodeStorage<State> _nodeStorage;
        ILogger _logger;
        IStateMachine<State> _stateMachine;
        NodeStateService _nodeStateService;

        public Snapshotter(ILogger<Snapshotter<State>> logger,
            INodeStorage<State> nodeStorage,
            IStateMachine<State> stateMachine,
            NodeStateService nodeStateService)
        {
            _logger = logger;
            _stateMachine = stateMachine;
            _nodeStorage = nodeStorage;
            _nodeStateService = nodeStateService;
        }


        public bool InstallSnapshot(object snapshot, int lastIncludedIndex, int lastIncludedTerm)
        {
            if (_nodeStorage.GetTotalLogCount() < lastIncludedIndex)
            {
                _logger.LogInformation("Installing snapshot...");
                _nodeStorage.SetLastSnapshot(((JObject)snapshot).ToObject<State>(), lastIncludedIndex, lastIncludedTerm);
                _stateMachine.ApplySnapshotToStateMachine(((JObject)snapshot).ToObject<State>());
                _nodeStateService.CommitIndex = lastIncludedIndex;
                _nodeStorage.SetCurrentTerm(lastIncludedTerm);
            }
            return true;
        }

        public void CheckAndApplySnapshots(int commitIndex, int snapshottingtrailinglogcount, int snapshotInterval)
        {
            var snapshotTo = commitIndex - snapshottingtrailinglogcount;
            // If the number of logs that are commited but not included in the snapshot are not included in interval, create snapshot
            if (snapshotInterval < (commitIndex - _nodeStorage.LastSnapshotIncludedIndex))
            {
                _logger.LogInformation("Reached snapshotting interval, creating snapshot to index " + snapshotTo + ".");
                _nodeStorage.CreateSnapshot(snapshotTo);
            }
        }
    }
}
