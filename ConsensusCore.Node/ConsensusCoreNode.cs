using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Messages;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node
{
    public class ConsensusCoreNode<Command, State, Repository>
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where Repository : BaseRepository<Command>
    {
        private Timer _heartbeatTimer;
        private Timer _electionTimeoutTimer;

        private NodeOptions _nodeOptions { get; }
        private ClusterOptions _clusterOptions { get; }
        private NodeStorage<Command, Repository> _nodeStorage { get; }
        public NodeState CurrentState { get; private set; }
        public ILogger<ConsensusCoreNode<Command, State, Repository>> Logger { get; }

        public Dictionary<string, HttpNodeConnector> NodeConnectors { get; private set; } = new Dictionary<string, HttpNodeConnector>();
        public Dictionary<Guid, string> NodeNextIndexes { get; private set; }

        public StateMachine<Command, State> _stateMachine { get; private set; }
        public Guid LeaderId { get; private set; }
        public string MyUrl { get; private set; }
        public Thread BootstrapThread;
        public bool IsBootstrapped = false;

        /// <summary>
        /// What logs have been commited to the state
        /// </summary>
        public int CommitIndex { get; private set; }
        public NodeInfo NodeInfo
        {
            get
            {
                return new NodeInfo()
                {
                    Id = _nodeStorage.Id
                };
            }
        }

        public ConsensusCoreNode(
           IOptions<ClusterOptions> clusterOptions,
            IOptions<NodeOptions> nodeOptions,
            NodeStorage<Command, Repository> nodeStorage,
            ILogger<ConsensusCoreNode<Command,
            State,
            Repository>> logger)
        {
            _nodeOptions = nodeOptions.Value;
            _clusterOptions = clusterOptions.Value;
            _nodeStorage = nodeStorage;
            Logger = logger;

            _electionTimeoutTimer = new Timer(ElectionTimeoutEventHandler);
            _heartbeatTimer = new Timer(HeartbeatTimeoutEventHandler);
            SetNodeRole(NodeState.Follower);

            BootstrapThread = new Thread(() =>
            {
                //Wait for the rest of the node to bootup
                Thread.Sleep(3000);
                BootstrapNode();
                IsBootstrapped = true;
            });

            BootstrapThread.Start();
        }

        public void BootstrapNode()
        {
            Logger.LogInformation("Bootstrapping Node!");

            foreach (var url in _clusterOptions.NodeUrls)
            {
                var testConnector = new HttpNodeConnector(url, TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs));

                Guid? nodeUrl = null;
                try
                {
                    nodeUrl = testConnector.GetNodeInfoAsync().GetAwaiter().GetResult().Id;
                }
                catch (Exception e)
                {
                    Logger.LogWarning("Node at url " + url + " was unreachable...");
                }

                if (nodeUrl != _nodeStorage.Id)
                {
                    NodeConnectors.Add(url, testConnector);
                }
                else
                {
                    MyUrl = url;
                }
            }

            if (MyUrl == null)
            {
                Logger.LogWarning("Node is not discoverable from the given node urls!");
            }
        }

        public void ElectionTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                Logger.LogDebug("Detected election timeout event.");
                SetNodeRole(NodeState.Candidate);
                var totalVotes = 1;

                Parallel.ForEach(NodeConnectors, connector =>
                {
                    try
                    {
                        var result = connector.Value.SendRequestVote(_nodeStorage.CurrentTerm, _nodeStorage.Id, _nodeStorage.GetLastLogIndex(), _nodeStorage.GetLastLogTerm()).GetAwaiter().GetResult();

                        if (result.Success)
                        {
                            Interlocked.Increment(ref totalVotes);
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.LogWarning("Encountered error while getting vote from node " + connector.Key + ", request failed with error \"" + e.Message + "\"");
                    }
                });

                if (totalVotes >= _clusterOptions.MinimumNodes)
                {
                    Logger.LogInformation("Recieved enough votes to be promoted, promoting to leader.");
                    LeaderId = LeaderId;
                    SetNodeRole(NodeState.Leader);
                }
            }
        }

        public void HeartbeatTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                Logger.LogDebug("Detected heartbeat timeout event.");
                SendHeartbeats();
            }
        }

        public void SendHeartbeats()
        {
            Logger.LogDebug("Sending heartbeats");
        }

        public void SetNodeRole(NodeState newState)
        {
            if (newState != CurrentState)
            {
                Logger.LogInformation("Node's role changed to " + newState.ToString());
                CurrentState = newState;

                switch (newState)
                {
                    case NodeState.Candidate:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        break;
                    case NodeState.Follower:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs, _clusterOptions.ElectionTimeoutMs);
                        StopTimer(_heartbeatTimer);
                        break;
                    case NodeState.Leader:
                        ResetTimer(_heartbeatTimer, _clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs);
                        StopTimer(_electionTimeoutTimer);
                        break;
                }
            }
        }

        private void ResetTimer(Timer timer, int dueTime, int period)
        {
            timer.Change(dueTime, period);
        }

        private void StopTimer(Timer timer)
        {
            ResetTimer(timer, Timeout.Infinite, Timeout.Infinite);
        }

        public bool RequestVote(RequestVote requestVoteRPC)
        {
            //Ref1 $5.2, $5.4
            if (_nodeStorage.CurrentTerm < requestVoteRPC.Term && ((_nodeStorage.VotedFor == null || _nodeStorage.VotedFor == requestVoteRPC.CandidateId) &&
                (requestVoteRPC.LastLogIndex >= _nodeStorage.GetLogCount() - 1 && requestVoteRPC.LastLogTerm >= _nodeStorage.GetLastLogTerm())))
            {
                return true;
            }
            return false;
        }

        public bool AppendEntry(AppendEntry<Command> entry)
        {

            if (entry.Term < _nodeStorage.CurrentTerm)
            {
                return false;
            }

            var previousEntry = _nodeStorage.GetLogAtIndex(entry.PrevLogIndex);


            if (previousEntry == null && entry.PrevLogIndex != 0)
            {
                Logger.LogWarning("Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");
                throw new MissingLogEntryException()
                {
                    LastLogEntryIndex = _nodeStorage.GetLogCount()
                };
            }

            if (previousEntry != null && previousEntry.Term != entry.PrevLogTerm)
            {
                Logger.LogWarning("Inconsistency found in the node logs and leaders logs, log " + entry.PrevLogTerm + " from term " + entry.PrevLogTerm + " does not exist.");
                throw new ConflictingLogEntryException()
                {
                    ConflictingTerm = entry.PrevLogTerm,
                    FirstTermIndex = _nodeStorage.Logs.Where(l => l.Term == entry.PrevLogTerm).First().Index
                };
            }

            _nodeStorage.AddLogs(entry.Entries);

            if (CommitIndex < entry.LeaderCommit)
            {
                Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                var allLogsToBeCommited = _nodeStorage.Logs.Where(l => l.Index > CommitIndex && l.Index >= entry.LeaderCommit);
                _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
            }
            return true;
        }

        public State GetState()
        {
            return _stateMachine.CurrentState;
        }
    }
}
