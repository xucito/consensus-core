using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Communication.Clients;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsensusCore.Node.Services.Raft
{
    public class CommitService<State>: BaseService<State> where State: BaseState, new()
    {
        public INodeStorage<State> _nodeStorage;
        public CommitService(ILogger<CommitService<State>> logger, ClusterConnectionPool clusterConnectionPool, ClusterOptions clusterOptions, NodeOptions nodeOptions, INodeStorage<State> nodeStorage, IStateMachine<State> stateMachine, NodeStateService nodeStateService) : base(logger, clusterOptions, nodeOptions, stateMachine, nodeStateService)
        {
            _nodeStorage = nodeStorage;
        }

        /// <summary>
        /// Apply outstanding commits
        /// </summary>
        public void ApplyCommits()
        {
            try
            {
                //As leader, calculate the commit index
                if (NodeStateService.Role == NodeState.Leader)
                {
                    var indexToAddTo = _nodeStorage.GetLastLogIndex();
                    while (NodeStateService.CommitIndex < indexToAddTo)
                    {
                        if (NodeStateService.MatchIndex.Values.Count(x => x >= indexToAddTo) >= (ClusterOptions.MinimumNodes - 1))
                        {
                            //You can catch this error as presumably all nodes in cluster wil experience the same error
                            try
                            {
                                StateMachine.ApplyLogsToStateMachine(_nodeStorage.GetLogRange(NodeStateService.CommitIndex + 1, indexToAddTo));// _nodeStorage.GetLogAtIndex(CommitIndex + 1));
                            }
                            catch (Exception e)
                            {
                                Logger.LogError(e.Message);
                            }
                            NodeStateService.CommitIndex = indexToAddTo;
                            NodeStateService.LatestLeaderCommit = indexToAddTo;
                        }
                        else
                        {
                        }
                        indexToAddTo--;
                    }
                }
                else if (NodeStateService.Role == NodeState.Follower)
                {
                    if (NodeStateService.CommitIndex < NodeStateService.LatestLeaderCommit)
                    {
                        var numberOfLogs = _nodeStorage.GetTotalLogCount();
                        //On resync, the commit index could be higher then the local amount of logs available
                        var commitIndexToSyncTill = numberOfLogs < NodeStateService.LatestLeaderCommit ? numberOfLogs : NodeStateService.LatestLeaderCommit;

                        if (_nodeStorage.LogExists(NodeStateService.CommitIndex + 1))
                        {
                            var allLogsToBeCommited = _nodeStorage.GetLogRange(NodeStateService.CommitIndex + 1, commitIndexToSyncTill);
                            try
                            {
                                StateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                            }
                            catch (Exception e)
                            {
                                Logger.LogError(e.Message);
                            }
                            NodeStateService.CommitIndex = allLogsToBeCommited.Last().Index;
                        }
                        else
                        {
                            Logger.LogWarning("Waiting for log " + (NodeStateService.CommitIndex + 1));
                        }
                    }
                }

                if (NodeStateService.Role == NodeState.Leader || NodeStateService.Role == NodeState.Follower)
                {
                    var snapshotTo = NodeStateService.CommitIndex - ClusterOptions.SnapshottingTrailingLogCount;
                    // If the number of logs that are commited but not included in the snapshot are not included in interval, create snapshot
                    if (ClusterOptions.SnapshottingInterval < (NodeStateService.CommitIndex - _nodeStorage.LastSnapshotIncludedIndex) && _nodeStorage.LogExists(_nodeStorage.LastSnapshotIncludedIndex + 1))
                    {
                        Logger.LogInformation("Reached snapshotting interval, creating snapshot to index " + snapshotTo + ".");
                        _nodeStorage.CreateSnapshot(snapshotTo);
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogWarning(" encountered error " + e.Message + " with stacktrace " + e.StackTrace);
            }
        }
    }
}
