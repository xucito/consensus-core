using ConsensusCore.BaseClasses;
using ConsensusCore.Clients;
using ConsensusCore.Enums;
using ConsensusCore.Exceptions;
using ConsensusCore.Interfaces;
using ConsensusCore.Messages;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using ConsensusCore.ViewModels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Services
{
    public class NodeManagerBAD<Command, State, StateMachine, Repository> :
        INodeManager<Command, State, StateMachine, Repository>
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where StateMachine : StateMachine<Command, State>
        where Repository : INodeRepository<Command>
    {
        public NodeInfo<Command> Information { get; }
        public NodeOptions _options { get; }
        public ClusterOptions _clusterOptions { get; }
        public INodeRepository<Command> _repository { get; }
        public NodeRole CurrentRole { get; set; }
        public object RoleLocker = new object();
        public KeyValuePair<Guid, string> CurrentLeader { get; set; }
        public bool IsInElection { get; set; }
        public ILogger<NodeManager<Command, State, StateMachine, Repository>> Logger { get; set; }
        public Thread ElectionThread;
        public DateTime LastAppendEntry { get; set; } 
        public object LastAppendEntryLock = new object();
        public string MyUrl { get; set; }
        private StateMachine<Command, State> _stateMachine { get; set; }
        public int CommitIndex { get; set; }
        public ConcurrentDictionary<string, int> FollowersLastIndex = new ConcurrentDictionary<string, int>();
        public ConcurrentDictionary<string, Thread> AppendEntryThreads = new ConcurrentDictionary<string, Thread>();

        public NodeManagerBAD(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<NodeOptions> options,
            INodeRepository<Command> repository,
            ILogger<NodeManager<Command,
            State,
            StateMachine,
            Repository>> logger)
        {
            _options = options.Value;
            _clusterOptions = clusterOptions.Value;
            _repository = repository;
            Information = repository.LoadConfiguration();
            ConsensusCoreNodeClient.SetTimeout(TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs));
            CurrentRole = NodeRole.Follower;
            Logger = logger;
            _stateMachine = new StateMachine<Command, State>(Information.Logs);
            if (Information.VotedFor == null)
            {
                IsInElection = true;
                ElectionThread = new Thread(() =>
                {
                    Random random = new Random();
                    Thread.CurrentThread.IsBackground = true;
                    Thread.Sleep(5000);
                    MyUrl = FindUrl(Information.Id).GetAwaiter().GetResult();

                    while (true)
                    {
                        while (CurrentRole == NodeRole.Candidate)
                        {
                            lock (RoleLocker)
                            {
                                Information.CurrentTerm++;
                                Logger.LogInformation("No leader detected, starting election for term " + Information.CurrentTerm);
                                Information.VotedFor = Information.Id;
                                var isLeader = BeginElection().GetAwaiter().GetResult();

                                if (isLeader)
                                {
                                    CurrentRole = NodeRole.Leader;
                                    IsInElection = true;
                                    //Reset the followers index
                                    FollowersLastIndex = new ConcurrentDictionary<string, int>();
                                    foreach (var url in _clusterOptions.NodeUrls)
                                    {
                                        //Assume all followers are upto date
                                        FollowersLastIndex.TryAdd(url, Information.Logs.Count());
                                    }
                                }
                                else
                                {
                                    Information.VotedFor = null;
                                    CurrentRole = NodeRole.Follower;
                                    Thread.Sleep(random.Next(0, 3000));
                                    if (Information.VotedFor == null)
                                    {
                                        CurrentRole = NodeRole.Candidate;
                                    }
                                    else
                                    {
                                        LastAppendEntry = DateTime.UtcNow;
                                    }
                                }
                            }
                        }

                        while (CurrentRole == NodeRole.Follower)
                        {
                            Thread.Sleep(_clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
                            lock (LastAppendEntryLock)
                            {
                                if ((DateTime.UtcNow - LastAppendEntry).TotalMilliseconds > _clusterOptions.ElectionTimeoutMs)
                                {
                                    Logger.LogWarning("No heartbeat from leader detected");
                                    if (_options.EnableLeader)
                                    {
                                        Logger.LogWarning("Becoming Candidate");
                                        CurrentRole = NodeRole.Candidate;
                                    }
                                    else
                                    {
                                        Logger.LogInformation("Node is not enabled for becoming leader, waiting for another candidate.");
                                    }
                                }
                                else
                                {
                                    Logger.LogInformation("Received successful heartbeat from " + CurrentLeader.Key);
                                }
                            }
                        }

                        while (CurrentRole == NodeRole.Leader)
                        {
                            Logger.LogInformation("Sending heartbeat for term " + Information.CurrentTerm);
                            foreach (var url in _clusterOptions.NodeUrls.Where(url => url != MyUrl))
                            {
                                var result = ProcessLogEntriesAsync(new List<LogEntry<Command>>()
                                {

                                }).GetAwaiter().GetResult();
                            }
                            Thread.Sleep((_clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs) / 2);
                        }
                    }
                });
                ElectionThread.Start();
            }
        }

        public async Task<string> FindUrl(Guid nodeId)
        {
            foreach (var url in _clusterOptions.NodeUrls)
            {
                try
                {
                    var result = await ConsensusCoreNodeClient.GetNodeInfoAsync(url);

                    if (result.Id == nodeId)
                    {
                        return url;
                    }
                }
                catch (Exception e)
                {
                    Logger.LogDebug("Found error " + e.Message + " while trying to identify my own URL.");
                }
            }
            throw new InvalidClusterUrlsException("Node is not apart of the given cluster URLs");
        }

        public bool AppendEntry(AppendEntry<Command> entry)
        {
            lock (LastAppendEntryLock)
            {
                lock (RoleLocker)
                {
                    if (entry.Term < Information.CurrentTerm)
                    {
                        return false;
                    }

                    var previousEntry = Information.GetLogAtIndex(entry.PrevLogIndex);


                    if (previousEntry == null && entry.PrevLogIndex != 0)
                    {
                        Logger.LogWarning("Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");
                        throw new MissingLogEntryException()
                        {
                            LastLogEntryIndex = Information.Logs.Count()
                        };
                    }

                    if (previousEntry != null && previousEntry.Term != entry.PrevLogTerm)
                    {
                        Logger.LogWarning("Inconsistency found in the node logs and leaders logs, log " + entry.PrevLogTerm + " from term " + entry.PrevLogTerm + " does not exist.");
                        throw new ConflictingLogEntryException()
                        {
                            ConflictingTerm = entry.PrevLogTerm,
                            FirstTermIndex = Information.Logs.Where(l => l.Term == entry.PrevLogTerm).First().Index
                        };
                    }

                    LastAppendEntry = DateTime.UtcNow;
                    Information.AddLogs(entry.Entries);

                    if (CommitIndex < entry.LeaderCommit)
                    {
                        Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                        var allLogsToBeCommited = Information.Logs.Where(l => l.Index > CommitIndex && l.Index >= entry.LeaderCommit);
                        _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                    }
                    return true;

                    /*
                    var matchingPreviousLog = Information.Logs.Where(l => (l.Index == entry.PrevLogIndex) && l.Term == entry.PrevLogTerm).FirstOrDefault();
                    if ((entry.LeaderId == CurrentLeader.Key || entry.Term >= Information.CurrentTerm) && (matchingPreviousLog != null || (entry.PrevLogIndex == 0 && Information.Logs.Count() == 0)) )
                    {
                        Logger.LogDebug("Received a append entry from leader " + entry.LeaderId + " for term " + entry.Term + ", commit " + entry.LeaderCommit + ".");
                        LastAppendEntry = DateTime.UtcNow;
                        Information.Logs.AddRange(entry.Entries);


                        if ((CurrentRole == NodeRole.Candidate || CurrentLeader.Key != entry.LeaderId))
                        {
                            CurrentRole = NodeRole.Follower;
                            CurrentLeader = new KeyValuePair<Guid, string>(entry.LeaderId, FindUrl(entry.LeaderId).GetAwaiter().GetResult());
                        }
                        
                        if (CommitIndex < entry.LeaderCommit)
                        {
                            Logger.LogDebug("Detected leader commit of " + entry.LeaderCommit + " commiting data on node.");
                            var allLogsToBeCommited = Information.Logs.Where(l => l.Index > CommitIndex && l.Index >= entry.LeaderCommit);
                            _stateMachine.ApplyLogsToStateMachine(allLogsToBeCommited);
                        }

                        return true;
                    }
                    else
                    {
                        if (Information.Logs.Count() < entry.PrevLogIndex)
                        {
                            Logger.LogWarning("Missing previous entry at index " + entry.PrevLogIndex + " from term " + entry.PrevLogTerm + " does not exist.");
                            throw new MissingLogEntryException()
                            {
                                LastLogEntryIndex = Information.Logs.Count()
                            };
                        }
                        else if (matchingPreviousLog == null)
                        {
                            Logger.LogWarning("Inconsistency found in the node logs and leaders logs, log " + entry.PrevLogTerm + " from term " + entry.PrevLogTerm + " does not exist.");
                            throw new ConflictingLogEntryException()
                            {
                                ConflictingTerm = entry.PrevLogTerm,
                                FirstTermIndex = Information.Logs.Where(l => l.Term == entry.PrevLogTerm).First().Index
                            };
                        }
                        else
                        {
                            Logger.LogWarning("Append RPC sent by " + entry.LeaderId + " who is not the current leader. Current leader is " + CurrentLeader.Key);
                        }
                        return false;
                }
                */
                }
            }
        }

        public bool PropogateNewAppendEntry(AppendEntry<Command> entry)
        {
            //Only send this request to nodes without an existing AppendEntryRequest
            foreach (var url in _clusterOptions.NodeUrls.Where(url => url != MyUrl && (!AppendEntryThreads.ContainsKey(url) || !AppendEntryThreads[url].IsAlive)))
            {
                try
                {
                    AppendEntryThreads.AddOrUpdate(url, new Thread(async () =>
                    {
                        await UpdateFollowerLogs(url);
                    }), (key, oldValue) =>
                    {
                        if (!oldValue.IsAlive)
                        {
                            return new Thread(() =>
                            {
                                UpdateFollowerLogs(url).GetAwaiter().GetResult();
                            });
                        }
                        else
                        {
                            throw new Exception("Two append entries were initiated at once");
                        }
                    });

                    AppendEntryThreads[url].Start();
                }
                catch (Exception e)
                {
                    throw e;
                }
            }

            int acknowledgedNodes = 0;
            do
            {
                foreach (var url in _clusterOptions.NodeUrls.Where(url => url != MyUrl && (!AppendEntryThreads.ContainsKey(url) || !AppendEntryThreads[url].IsAlive)))
                {
                    AppendEntryThreads[url].Join();
                }

                acknowledgedNodes = FollowersLastIndex.Where(fli => fli.Value == Information.Logs.Count()).Count();
                if (acknowledgedNodes < _clusterOptions.MinimumNodes - 1)
                {
                    Thread.Sleep(1000);
                    Logger.LogDebug("Only " + acknowledgedNodes + " have acknowledged the request.");
                }
            }
            while (acknowledgedNodes < _clusterOptions.MinimumNodes - 1);

            return true;
        }

        private async Task<bool> UpdateFollowerLogs(string url)
        {
            while (FollowersLastIndex[url] < Information.Logs.Count())
            {
                try
                {
                    var lastValidLog = Information.GetLogAtIndex(FollowersLastIndex[url]);

                    var result = await ConsensusCoreNodeClient.SendAppendEntry<Command>(url, new AppendEntry<Command>()
                    {
                        Entries = Information.Logs.Where(e => e.Index > FollowersLastIndex[url]).ToList(),
                        LeaderCommit = CommitIndex,
                        LeaderId = Information.Id,
                        PrevLogIndex = lastValidLog == null ? 0 : lastValidLog.Index,
                        PrevLogTerm = lastValidLog == null ? 0 : lastValidLog.Term,
                        Term = Information.CurrentTerm
                    });

                    if (result)
                    {
                        FollowersLastIndex[url] = FollowersLastIndex[url] + 1;
                    }
                }
                catch (ConflictingLogEntryException e)
                {
                    Logger.LogInformation("Detected conflicts in follower " + url + " rolling back the nodes logs.");
                    FollowersLastIndex[url] = Information.Logs.Where(l => l.Term == e.ConflictingTerm).First().Index;
                }
                catch (MissingLogEntryException e)
                {
                    Logger.LogInformation("Detected missing logs in follower " + url + " sending logs from " + e.LastLogEntryIndex);
                    FollowersLastIndex[url] = e.LastLogEntryIndex;
                }
                catch (Exception e)
                {
                    Logger.LogError("Unexpected error " + e.Message);
                    return false;
                }
            }
            return true;
        }

        public bool RequestVote(RequestVote vote)
        {
            if (Information.CurrentTerm < vote.Term)
            {
                if (Information.Logs.Count() <= vote.LastLogIndex && (Information.VotedFor == null || Information.VotedFor == vote.CandidateId))
                {
                    Logger.LogInformation("Voted for " + vote + " for term " + vote.Term);
                    Information.VotedFor = vote.CandidateId;
                    Information.CurrentTerm = vote.Term;
                    return true;
                }
            }
            return false;
        }

        public async Task<bool> BeginElection()
        {
            int TotalVotes = 1;
            foreach (var url in _clusterOptions.NodeUrls.Where(url => url != MyUrl))
            {
                try
                {
                    var result = await ConsensusCoreNodeClient.SendRequestVote(url, new Messages.RequestVote()
                    {
                        CandidateId = Information.Id,
                        Term = Information.CurrentTerm
                    });

                    if (result.Success && result.NodeId != Information.Id)
                    {
                        TotalVotes++;
                        Logger.LogInformation("Received vote from " + url + ". Total votes " + TotalVotes);
                    }

                }
                catch (Exception e)
                {
                    Logger.LogError("Encountered error " + e.Message + " while collecting vote from " + url);
                }
            }

            if (TotalVotes >= _clusterOptions.MinimumNodes)
            {
                Logger.LogInformation("Received minimum votes to become a leader. Promoting now.");
                return true;
            }
            else
            {
                Logger.LogWarning("Minimum amount of votes was not established for term " + Information.CurrentTerm + ", timing out and re-running election");
                return false;
            }
        }

        public async Task<bool> ProcessCommandsAsync(List<Command> commands, int timeoutMs = 10000)
        {
            var startTime = DateTime.UtcNow;

            while (CurrentRole == NodeRole.Candidate)
            {
                if ((DateTime.UtcNow - startTime).TotalMilliseconds > timeoutMs)
                {
                    return false;
                }
            }

            if (CurrentRole == NodeRole.Leader)
            {
                return await ProcessLogEntriesAsync(new List<LogEntry<Command>>() {
                    new LogEntry<Command>()
                    {
                        Commands = commands,
                        Index = Information.Logs.Count() + 1,
                        Term = Information.CurrentTerm
                    }
                });
            }
            else if (CurrentRole == NodeRole.Follower)
            {
                //TODO FIX THIS ROUTING
                Logger.LogDebug("Received a command as a follower, rerouting to leader " + CurrentLeader.Key + "@" + CurrentLeader.Value + ".");
                await ConsensusCoreNodeClient.RouteCommands<Command>(CurrentLeader.Value, null);
            }

            return true;
        }

        /// <summary>
        /// Only the leader can process entries
        /// </summary>
        /// <param name="entries"></param>
        /// <param name="timeoutMs"></param>
        /// <returns></returns>
        public async Task<bool> ProcessLogEntriesAsync(List<LogEntry<Command>> entries, int timeoutMs = 10000)
        {
            var lastLog = Information.Logs.LastOrDefault();
            var appendEntry = new AppendEntry<Command>()
            {
                Entries = entries,
                LeaderId = Information.Id,
                LeaderCommit = CommitIndex,
                PrevLogIndex = lastLog != null ? lastLog.Index : 0,
                Term = Information.CurrentTerm,
                PrevLogTerm = lastLog != null ? lastLog.Term : 0
            };
            if (entries.Count() > 0)
            {
                Information.Logs.AddRange(appendEntry.Entries);
            }
            //var isAcknowledged = await PropogateNewAppendEntry(appendEntry);
            var isAcknowledged = PropogateNewAppendEntry(appendEntry);

            if (isAcknowledged)
            {
                if (entries.Count() > 0)
                {
                    Logger.LogDebug("Entry " + entries.Last() + " is acknowledged by the cluster.");
                    _stateMachine.ApplyLogsToStateMachine(appendEntry.Entries);
                    CommitIndex = entries.Last().Index;
                }
                else
                {
                    Logger.LogDebug("Heartbeat was acknowledged by the cluster.");
                }
            }
            else
            {
                Logger.LogWarning("Entry " + appendEntry.LeaderCommit + " is not acknowledged by the cluster.");
            }

            return false;
        }

        public State GetState()
        {
            return _stateMachine.CurrentState;
        }
    }
}
