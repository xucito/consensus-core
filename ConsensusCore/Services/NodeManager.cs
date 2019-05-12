using ConsensusCore.Clients;
using ConsensusCore.Enums;
using ConsensusCore.Exceptions;
using ConsensusCore.Messages;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using ConsensusCore.ViewModels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Services
{
    public class NodeManager<T> : INodeManager where T : INodeRepository
    {
        public NodeInfo Information { get; }
        public NodeOptions _options { get; }
        public ClusterOptions _clusterOptions { get; }
        public INodeRepository _repository { get; }
        public NodeRole CurrentRole { get; set; }
        public object RoleLocker = new object();

        //  public Guid VotedFor { get; set; }
        public Guid CurrentLeader { get; set; }
        public bool IsInElection { get; set; }
        public ILogger<NodeManager<T>> Logger { get; set; }

        public Thread ElectionThread;
        public DateTime LastAppendEntry { get; set; }

        public object LastAppendEntryLock = new object();

        public string MyUrl { get; set; }

        public NodeManager(IOptions<ClusterOptions> clusterOptions, IOptions<NodeOptions> options, INodeRepository repository, ILogger<NodeManager<T>> logger)
        {
            _options = options.Value;
            _clusterOptions = clusterOptions.Value;
            _repository = repository;
            Information = repository.LoadConfiguration();
            ConsensusCoreNodeClient.SetTimeout(TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs));
            CurrentRole = NodeRole.Follower;
            Logger = logger;
            if (Information.VotedFor == null)
            {
                IsInElection = true;
                ElectionThread = new Thread(() =>
                {
                    Random random = new Random();
                    Thread.CurrentThread.IsBackground = true;

                    Thread.Sleep(5000);
                    MyUrl = FindMyUrl().GetAwaiter().GetResult();

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
                            lock (LastAppendEntryLock)
                            {
                                if ((DateTime.UtcNow - LastAppendEntry).TotalMilliseconds > _clusterOptions.ElectionTimeoutMs)
                                {
                                    Logger.LogWarning("No heartbeat from leader detected, becoming Candidate");
                                    CurrentRole = NodeRole.Candidate;
                                }
                                else
                                {
                                    Logger.LogInformation("Received successful heartbeat from " + Information.VotedFor);
                                }
                            }
                            Thread.Sleep(_clusterOptions.ElectionTimeoutMs + _clusterOptions.LatencyToleranceMs);
                        }

                        while (CurrentRole == NodeRole.Leader)
                        {
                            Logger.LogInformation("Sending heartbeat for term " + Information.CurrentTerm);
                            SendAppendEntry(new AppendEntry()
                            {
                                LeaderId = Information.Id,
                                Term = Information.CurrentTerm
                            });
                            Thread.Sleep(_clusterOptions.ElectionTimeoutMs - _clusterOptions.LatencyToleranceMs);
                        }
                    }
                });
                ElectionThread.Start();
            }
        }

        public async Task<string> FindMyUrl()
        {
            foreach (var url in _clusterOptions.NodeUrls)
            {
                try
                {
                    var result = await ConsensusCoreNodeClient.GetNodeInfoAsync(url);

                    if (result.Id == Information.Id)
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

        public void AppendEntry(AppendEntry entry)
        {
            lock (LastAppendEntryLock)
            {
                lock (RoleLocker)
                {
                    if (entry.LeaderId == Information.VotedFor || entry.Term > Information.CurrentTerm)
                    {
                        LastAppendEntry = DateTime.UtcNow;

                        if ((CurrentRole == NodeRole.Candidate || Information.VotedFor == null) && entry.Term > Information.CurrentTerm)
                        {
                            CurrentRole = NodeRole.Follower;
                            Information.VotedFor = entry.LeaderId;
                        }
                    }
                }
            }
        }

        public async void SendAppendEntry(AppendEntry entry)
        {
            var validReplies = 1;
            foreach (var url in _clusterOptions.NodeUrls.Where(url => url != MyUrl))
            {
                try
                {
                    var result = await ConsensusCoreNodeClient.SendAppendEntry(url, entry);
                    validReplies++;
                }
                catch (Exception e)
                {
                }
            }

            if(validReplies < _clusterOptions.MinimumNodes && CurrentRole != NodeRole.Candidate)
            {
                CurrentRole = NodeRole.Candidate;
                Logger.LogWarning("Not enough followers have replied, returning to candidate stage for new election.");
            }
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
    }
}
