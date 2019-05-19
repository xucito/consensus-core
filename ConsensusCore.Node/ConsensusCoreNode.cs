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
    public class ConsensusCoreNode<Command, State, Repository> : IConsensusCoreNode<Command, State, Repository>
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where Repository : BaseRepository<Command>
    {
        private Timer _heartbeatTimer;
        private Timer _electionTimeoutTimer;
        private Thread _commitThread;

        private NodeOptions _nodeOptions { get; }
        private ClusterOptions _clusterOptions { get; }
        private NodeStorage<Command, Repository> _nodeStorage { get; }
        public NodeState CurrentState { get; private set; }
        public ILogger<ConsensusCoreNode<Command, State, Repository>> Logger { get; }

        public Dictionary<string, HttpNodeConnector> NodeConnectors { get; private set; } = new Dictionary<string, HttpNodeConnector>();
        public Dictionary<string, int> NextIndex { get; private set; } = new Dictionary<string, int>();
        public Dictionary<string, int> MatchIndex { get; private set; } = new Dictionary<string, int>();

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
            Repository>> logger,
            StateMachine<Command, State> stateMachine)
        {
            _nodeOptions = nodeOptions.Value;
            _clusterOptions = clusterOptions.Value;
            _nodeStorage = nodeStorage;
            Logger = logger;
            _electionTimeoutTimer = new Timer(ElectionTimeoutEventHandler);
            _heartbeatTimer = new Timer(HeartbeatTimeoutEventHandler);
            _stateMachine = stateMachine;
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

        public void ResetLeaderState()
        {
            NextIndex.Clear();
            MatchIndex.Clear();
            foreach (var url in _clusterOptions.NodeUrls)
            {
                NextIndex.Add(url, _nodeStorage.GetLogCount());
                MatchIndex.Add(url, 0);
            }
        }

        public Thread NewCommitThread()
        {
            return new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                while (CurrentState == NodeState.Leader)
                {
                    while (CommitIndex < _nodeStorage.GetLastLogIndex())
                    {
                        if (MatchIndex.Values.Count(x => x >= CommitIndex + 1) >= (_clusterOptions.MinimumNodes - 1))
                        {
                            _stateMachine.ApplyLogToStateMachine(_nodeStorage.GetLogAtIndex(CommitIndex + 1));
                            CommitIndex++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    Thread.Sleep(100);
                }
            });
        }

        public void BootstrapNode()
        {
            Logger.LogInformation("Bootstrapping Node!");

            while (MyUrl == null)
            {
                NodeConnectors.Clear();
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
        }

        public void ElectionTimeoutEventHandler(object args)
        {
            if (IsBootstrapped)
            {
                Logger.LogDebug("Detected election timeout event.");
                _nodeStorage.UpdateCurrentTerm(_nodeStorage.CurrentTerm + 1);
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
            Parallel.ForEach(NodeConnectors, connector =>
            {
                try
                {
                    var entriesToSend = new List<LogEntry<Command>>();

                    var prevLogIndex = Math.Max(0, NextIndex[connector.Key] - 1);
                    int prevLogTerm = (_nodeStorage.GetLogCount() > 0 && prevLogIndex > 0) ? prevLogTerm = _nodeStorage.GetLogAtIndex(prevLogIndex).Term : 0;

                    if (NextIndex[connector.Key] <= _nodeStorage.GetLastLogIndex() && _nodeStorage.GetLastLogIndex() != 0)
                    {
                        entriesToSend = _nodeStorage.Logs.Where(l => l.Index >= NextIndex[connector.Key]).ToList();
                        Logger.LogDebug("Detected node " + connector.Key + " is not upto date, sending logs from " + entriesToSend.First().Index + " to " + entriesToSend.Last().Index);
                    }

                    var result = connector.Value.SendAppendEntry<Command>(_nodeStorage.CurrentTerm, _nodeStorage.Id, prevLogIndex, prevLogTerm, entriesToSend, CommitIndex).GetAwaiter().GetResult();
                    if (entriesToSend.Count() > 0)
                    {
                        NextIndex[connector.Key] = entriesToSend.Last().Index + 1;
                        MatchIndex[connector.Key] = entriesToSend.Last().Index;
                    }
                }
                catch (ConflictingLogEntryException e)
                {
                    var firstEntryOfTerm = _nodeStorage.Logs.Where(l => l.Term == e.ConflictingTerm).FirstOrDefault();
                    var revertedIndex = firstEntryOfTerm.Index < e.FirstTermIndex ? firstEntryOfTerm.Index : e.FirstTermIndex;
                    Logger.LogWarning("Detected node " + connector.Value + " has conflicting values, reverting to " + revertedIndex);

                    //Revert back to the first index of that term
                    MatchIndex[connector.Key] = revertedIndex;
                }
                catch (MissingLogEntryException e)
                {
                    Logger.LogWarning("Detected node " + connector.Value + " is missing the previous log, sending logs from log " + e.LastLogEntryIndex + 1);
                    MatchIndex[connector.Key] = e.LastLogEntryIndex + 1;
                }
                catch (Exception e)
                {
                    Logger.LogWarning("Encountered error while sending heartbeat to node " + connector.Key + ", request failed with error \"" + e.Message + "\"" + e.StackTrace);

                }
            });
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
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
                        StopTimer(_heartbeatTimer);
                        break;
                    case NodeState.Follower:
                        ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
                        StopTimer(_heartbeatTimer);
                        break;
                    case NodeState.Leader:
                        ResetLeaderState();
                        ResetTimer(_heartbeatTimer, _clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs);
                        StopTimer(_electionTimeoutTimer);
                        _commitThread = NewCommitThread();
                        _commitThread.Start();
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
            if (IsBootstrapped)
            {
                //Ref1 $5.2, $5.4
                if (_nodeStorage.CurrentTerm < requestVoteRPC.Term && ((_nodeStorage.VotedFor == null || _nodeStorage.VotedFor == requestVoteRPC.CandidateId) &&
                (requestVoteRPC.LastLogIndex >= _nodeStorage.GetLogCount() - 1 && requestVoteRPC.LastLogTerm >= _nodeStorage.GetLastLogTerm())))
                {
                    _nodeStorage.SetVotedFor(requestVoteRPC.CandidateId);
                    Logger.LogInformation("Voting for " + requestVoteRPC.CandidateId + " for term " + requestVoteRPC.Term);
                    SetNodeRole(NodeState.Follower);
                    return true;
                }
                return false;
            }
            return false;
        }

        public bool AppendEntry(AppendEntry<Command> entry)
        {
            if (IsBootstrapped)
            {
                ResetTimer(_electionTimeoutTimer, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs, _clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
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

                SetNodeRole(NodeState.Follower);

                _nodeStorage.AddLogs(entry.Entries);

                if (CommitIndex < entry.LeaderCommit)
                {
                    Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                    var allLogsToBeCommited = _nodeStorage.Logs.Where(l => l.Index > CommitIndex && l.Index <= entry.LeaderCommit);
                    _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                    CommitIndex = entry.LeaderCommit;
                }
                return true;
            }
            return false;
        }

        public int AddCommand(List<Command> command, bool waitForCommitment = false)
        {
            if (CurrentState == NodeState.Leader)
            {
                var newLog = new LogEntry<Command>()
                {
                    Commands = command,
                    Index = _nodeStorage.GetLastLogIndex() + 1,
                    Term = _nodeStorage.CurrentTerm
                };
                _nodeStorage.AddLog(newLog);

                while (waitForCommitment)
                {
                    if (CommitIndex >= newLog.Index)
                    {
                        return newLog.Index;
                    }
                    else
                    {
                        Thread.Sleep(10);
                    }
                }

                return 0;
            }
            else
            {
                return 0;
            }
        }

        public State GetState()
        {
            return _stateMachine.CurrentState;
        }
    }
}
