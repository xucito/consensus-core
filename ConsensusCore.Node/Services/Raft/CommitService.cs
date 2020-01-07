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
        public ILogger<CommitService<State>> _logger; 
        public INodeStorage<State> _nodeStorage;
        public CommitService(ILogger<CommitService<State>> logger, ClusterOptions clusterOptions, NodeOptions nodeOptions, INodeStorage<State> nodeStorage, IStateMachine<State> stateMachine, NodeStateService nodeStateService) : base(logger, clusterOptions, nodeOptions, stateMachine, nodeStateService)
        {
            _nodeStorage = nodeStorage;
            _logger = logger;
        }

        /// <summary>
        /// Apply outstanding commits
        /// </summary>
        public void ApplyCommits()
        {
            try
            {
                var commitIndex = NodeStateService.CommitIndex;
                //As leader, calculate the commit index
                if (NodeStateService.Role == NodeState.Leader)
                {
                    var indexToAddTo = _nodeStorage.GetLastLogIndex();
                    while (commitIndex < indexToAddTo)
                    {
                        if (NodeStateService.MatchIndex.Values.Count(x => x >= indexToAddTo) >= (ClusterOptions.MinimumNodes - 1))
                        {
                            //You can catch this error as presumably all nodes in cluster wil experience the same error
                            try
                            {
                                _logger.LogDebug("Applying commit "+ (commitIndex + 1) + " to " + indexToAddTo);
                                StateMachine.ApplyLogsToStateMachine(_nodeStorage.GetLogRange(commitIndex + 1, indexToAddTo));// _nodeStorage.GetLogAtIndex(CommitIndex + 1));
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
                    if (commitIndex < NodeStateService.LatestLeaderCommit)
                    {
                        var numberOfLogs = _nodeStorage.GetTotalLogCount();
                        //On resync, the commit index could be higher then the local amount of logs available
                        var commitIndexToSyncTill = numberOfLogs < NodeStateService.LatestLeaderCommit ? numberOfLogs : NodeStateService.LatestLeaderCommit;

                        _logger.LogDebug("Applying commit " + (commitIndex + 1) + " to " + commitIndexToSyncTill);
                        if (_nodeStorage.LogExists(commitIndex + 1))
                        {
                            var allLogsToBeCommited = _nodeStorage.GetLogRange(commitIndex + 1, commitIndexToSyncTill);
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
                            Logger.LogDebug("Waiting for log " + (NodeStateService.CommitIndex + 1));
                        }
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
