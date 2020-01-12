using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Communication.Clients;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
            var snapshotTo = commitIndex - 3;
            // If the number of logs that are commited but not included in the snapshot are not included in interval, create snapshot
            if (snapshotInterval < (commitIndex - _nodeStorage.LastSnapshotIncludedIndex))
            {
                /*_logger.LogInformation("Reached snapshotting interval, creating snapshot to index " + snapshotTo + ".");
                _logger.LogInformation("State before snapshotting " + _nodeStateService.CommitIndex);
                _logger.LogInformation(JsonConvert.SerializeObject(_stateMachine.CurrentState, Formatting.Indented));*/
                /* _logger.LogInformation("Logs before snapshotting ");
                 _logger.LogInformation(JsonConvert.SerializeObject(_nodeStorage.Logs, Formatting.Indented));
                 _logger.LogInformation("Snapshots before snapshotting ");
                 _logger.LogInformation(JsonConvert.SerializeObject(_nodeStorage.LastSnapshot, Formatting.Indented));
                 */


                //var startingState = JToken.Parse(JsonConvert.SerializeObject(_stateMachine.CurrentState));
                _nodeStorage.CreateSnapshot(snapshotTo, snapshottingtrailinglogcount);
                /* _logger.LogInformation("State after snapshotting " + _nodeStateService.CommitIndex);
                 _logger.LogInformation(JsonConvert.SerializeObject(_stateMachine.CurrentState, Formatting.Indented));*/

               // var endingState = JToken.Parse(JsonConvert.SerializeObject(_stateMachine.CurrentState));

                /*if(!JToken.DeepEquals(startingState, endingState))
                {
                    Console.WriteLine("");
                }*/

                /* _logger.LogInformation("Logs after snapshotting ");
                 _logger.LogInformation(JsonConvert.SerializeObject(_nodeStorage.Logs, Formatting.Indented));
                 _logger.LogInformation("Snapshots after snapshotting ");
                 _logger.LogInformation(JsonConvert.SerializeObject(_nodeStorage.LastSnapshot, Formatting.Indented));
     */
            }
        }
    }
}
