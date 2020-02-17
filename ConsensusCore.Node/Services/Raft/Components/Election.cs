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
    public class Election
    {
        private ILogger Logger;
        private TimeSpan _latencyTolerance;
        private IEnumerable<string> _members;

        public Election(ILogger<Election> logger, TimeSpan latencyTolerance, IEnumerable<string> members)
        {
            _latencyTolerance = latencyTolerance;
            _members = members;
            Logger = logger;
        }

        public async Task<ConcurrentDictionary<Guid, string>> CollectVotes(int currentTerm, Guid candidateid, int lastLogIndex, int lastLogTerm)
        {
            try
            {
                ConcurrentDictionary<Guid, string> nodes = new ConcurrentDictionary<Guid, string>();

                var tasks = _members.Select(async nodeUrl =>
                {
                    try
                    {
                        var testConnector = new HttpNodeConnector(nodeUrl, _latencyTolerance, TimeSpan.FromMilliseconds(-1));
                        var result = await testConnector.Send(new RequestVote()
                        {
                            Term = currentTerm,
                            CandidateId = candidateid,
                            LastLogIndex = lastLogIndex,
                            LastLogTerm = lastLogTerm
                        });

                        if (result.IsSuccessful)
                        {
                            var addResult = nodes.TryAdd(result.NodeId, nodeUrl);
                            if (!addResult)
                            {
                                Console.WriteLine("Failed to add " + result.NodeId + " url: " + nodeUrl);
                            }
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

                return nodes;
            }
            catch (Exception e)
            {
                throw new Exception("Failed to run election with error " + e.StackTrace);
            }
        }
    }
}
