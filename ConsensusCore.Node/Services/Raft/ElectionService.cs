using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs.Raft;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Communication.Clients;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Raft
{
    public class ElectionService<State> : BaseService<State> where State : BaseState, new()
    {
        ClusterConnectionPool _clusterConnectionPool;
        INodeStorage<State> _nodeStorage;

        public ElectionService(ILogger<ElectionService<State>> logger,
            ClusterConnectionPool clusterConnectionPool,
            ClusterOptions clusterOptions,
            NodeOptions nodeOptions,
            IStateMachine<State> stateMachine,
            INodeStorage<State> nodeStorage,
            NodeStateService nodeStateService) : base(logger, clusterOptions, nodeOptions, stateMachine, nodeStateService)
        {
            _clusterConnectionPool = clusterConnectionPool;
            _nodeStorage = nodeStorage;
        }

        public async Task<int> CollectVotes()
        {
            try
            {
                Logger.LogDebug(NodeStateService.GetNodeLogId() + "Starting election for term " + (_nodeStorage.CurrentTerm + 1) + ".");
                var lastLogTerm = _nodeStorage.GetLastLogTerm();
                // Caters for conditions when the node starts up and self elects to a greater term with two other nodes in Consensus
                if (_nodeStorage.CurrentTerm > lastLogTerm + 3)
                {
                    Logger.LogInformation("Detected that the node is too far ahead of its last log (3), restarting election from the term of the last log term " + lastLogTerm);
                    _nodeStorage.SetCurrentTerm(lastLogTerm);
                }
                else
                {
                    _nodeStorage.SetCurrentTerm(_nodeStorage.CurrentTerm + 1);
                }
                var totalVotes = 1;
                //Vote for yourself
                _nodeStorage.SetVotedFor(_nodeStorage.Id);
                ConcurrentDictionary<Guid, string> nodes = new ConcurrentDictionary<Guid, string>();

                var tasks = ClusterOptions.GetClusterUrls().Where(url => url != NodeStateService.Url).Select(async nodeUrl =>
                {
                    try
                    {
                        var testConnector = new HttpNodeConnector(nodeUrl, TimeSpan.FromMilliseconds(ClusterOptions.LatencyToleranceMs), TimeSpan.FromMilliseconds(ClusterOptions.DataTransferTimeoutMs));
                        var result = await testConnector.Send(new RequestVote()
                        {
                            Term = _nodeStorage.CurrentTerm,
                            CandidateId = _nodeStorage.Id,
                            LastLogIndex = _nodeStorage.GetLastLogIndex(),
                            LastLogTerm = _nodeStorage.GetLastLogTerm()
                        });

                        if (result.IsSuccessful)
                        {
                            var addResult = nodes.TryAdd(result.NodeId, nodeUrl);
                            if (!addResult)
                            {
                                Console.WriteLine("Failed to add " + result.NodeId + " url: " + nodeUrl);
                            }
                            Interlocked.Increment(ref totalVotes);
                        }
                        else
                        {
                            Logger.LogDebug("Node at " + nodeUrl + " rejected vote request.");
                        }
                    }
                    catch (TaskCanceledException e)
                    {
                        Logger.LogWarning("Encountered error while getting vote from node at " + nodeUrl + ", request timed out...");
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Encountered error while getting vote from node at " + nodeUrl + ", request failed with error \"" + e.Message + "\"");
                    }
                });

                await Task.WhenAll(tasks);

                return totalVotes;
            }
            catch (Exception e)
            {
                throw new Exception("Failed to run election with error " + e.StackTrace);
            }
        }
    }
}
