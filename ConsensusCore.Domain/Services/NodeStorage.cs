﻿using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.Utility;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Services
{
    public class NodeStorage<State> : INodeStorage<State> where State : BaseState, new()
    {
        private ILogger Logger { get; set; }
        public Guid Id { get; set; }
        public string Name { get; set; }
        public double Version { get; set; } = 1.0;
        public int CurrentTerm { get; set; } = 0;
        public Guid? VotedFor { get; set; } = null;

        /// <summary>
        /// The last term the snapshot was captured for
        /// </summary>
        public int LastSnapshotIncludedTerm { get; set; } = 0;
        /// <summary>
        /// The last index the snapshot was captured for, inclusive
        /// </summary>
        public int LastSnapshotIncludedIndex { get; set; } = 0;
        /// <summary>
        /// The last snapshot to apply logs to
        /// </summary>
        public State LastSnapshot { get; set; }
        public SortedList<int, LogEntry> Logs { get; set; } = new SortedList<int, LogEntry>();

        [JsonIgnore]
        public ConcurrentStack<BaseCommand> CommandsQueue = new ConcurrentStack<BaseCommand>();
        [JsonIgnore]
        private int _currentActiveLog = 0;
        [JsonIgnore]
        public object _locker = new object();
        [JsonIgnore]
        private IBaseRepository<State> _repository;
        [JsonIgnore]
        public bool RequireSave = false;
        [JsonIgnore]
        public readonly object _saveLocker = new object();
        [JsonIgnore]
        public Thread _saveThread;
        public Task _logConcatThread;

        public NodeStorage()
        {
        }


        public NodeStorage(ILogger<NodeStorage<State>> logger, IBaseRepository<State> repository)
        {
            Logger = logger;
            _repository = repository;

            var loadedData = repository.LoadNodeDataAsync().GetAwaiter().GetResult();
            if (loadedData != null)
            {
                Id = loadedData.Id;
                Name = loadedData.Name;
                Version = loadedData.Version;
                CurrentTerm = loadedData.CurrentTerm;
                VotedFor = loadedData.VotedFor;
                Logs = loadedData.Logs;
                SetLastSnapshot(loadedData.LastSnapshot, loadedData.LastSnapshotIncludedIndex, loadedData.LastSnapshotIncludedTerm);
            }
            else
            {
                Id = Guid.NewGuid();
                Logger.LogInformation("Failed to load local node storage from store, creating new node storage");
                Save();
            }

            _currentActiveLog = GetLastLogIndex() + 1;
            // CommandsQueue.TryAdd(_currentActiveLog, new ConcurrentQueue<BaseCommand>());

            if (_repository != null)
            {
                _saveThread = new Thread(async () =>
                {
                    await SaveThread();
                });
                _saveThread.Start();
            }

            //  _concatenateCommands = new Timer(CommitCommandsEventHandler);
            // _concatenateCommands.Change(0, 50);
            // var loadedData = _repository.LoadNodeData();
            _logConcatThread = Task.Run(() =>
            {
                Thread.Sleep(1000);
                Logger.LogInformation("Starting log concatenation.");
                ConcatenateLogs(); ;
            });
        }

        public void ConcatenateLogs()
        {
            while (true)
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var evaluatedLog = _currentActiveLog;
                var totalObjects = CommandsQueue.Count();
                totalObjects = totalObjects < 50 ? totalObjects : 50;
                if (totalObjects > 0)
                {
                    BaseCommand[] totalCommands = new BaseCommand[totalObjects];
                    CommandsQueue.TryPopRange(totalCommands, 0, totalObjects);
                    Logs.TryAdd(evaluatedLog, new LogEntry()
                    {
                        Commands = totalCommands.ToList(),
                        Term = CurrentTerm,
                        Index = evaluatedLog
                    });
                    _currentActiveLog += 1;
                    Save();
                    Logger.LogDebug("Adding logs for index " + evaluatedLog + " Log concatenation took " + stopwatch.ElapsedMilliseconds + " to add " + totalObjects);
                    //Console.WriteLine("Adding logs for index " + evaluatedLog + " Log concatenation took " + stopwatch.ElapsedMilliseconds + " to add " + totalObjects);
                }
                Thread.Sleep(10);
            }
        }

        /*public void CommitCommandsEventHandler(object args)
        {
            try
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var evaluatedLog = _currentActiveLog;
                CommandsQueue.TryAdd(evaluatedLog + 1, new ConcurrentQueue<BaseCommand>());
                //If there are logs pending
                if (CommandsQueue.ContainsKey(evaluatedLog) && CommandsQueue[evaluatedLog].Count() > 0 && evaluatedLog == (Interlocked.Increment(ref _currentActiveLog) - 1))
                {
                    var count = CommandsQueue[evaluatedLog].Count();
                    Logs.TryAdd(evaluatedLog, new LogEntry()
                    {
                        Commands = CommandsQueue[evaluatedLog].ToList(),
                        Term = CurrentTerm,
                        Index = evaluatedLog
                    });
                    CommandsQueue.Remove(evaluatedLog, out _);
                    Save();
                    Logger.LogDebug("Adding logs for index " + evaluatedLog + " Log concatenation took " + stopwatch.ElapsedMilliseconds + " to add " + count);
                    //Console.WriteLine("Log concatenation took " + stopwatch.ElapsedMilliseconds + " to add " + count);
                }
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to commit commands with error " + e.Message + Environment.StackTrace + e.StackTrace);
            }
        }*/

        public void SetLastSnapshot(State snapshot, int lastIncludedIndex, int lastIncludedTerm)
        {
            lock (_locker)
            {
                LastSnapshot = snapshot;
                LastSnapshotIncludedIndex = lastIncludedIndex;
                LastSnapshotIncludedTerm = lastIncludedTerm;
            }
            Save();
        }

        public int EnqueueCommand(BaseCommand[] commands)
        {
            CommandsQueue.PushRange(commands);
            return _currentActiveLog;
        }

        public void CreateSnapshot(int indexIncludedTo, int trailingLogCount)
        {
            var stateMachine = new StateMachine<State>();

            lock (_locker)
            {
                if (!LogExists(LastSnapshotIncludedIndex + 1))
                {
                    return;
                }

                //Find the log position of the commit index
                if (LastSnapshot != null)
                {
                    stateMachine.ApplySnapshotToStateMachine(SystemExtension.Clone(LastSnapshot));
                }

                var logIncludedTo = GetLogAtIndex(indexIncludedTo);
                var logIncludedFrom = GetLogAtIndex(LastSnapshotIncludedIndex + 1);
                Logger.LogDebug("Last snapshot index, getting logs " + logIncludedFrom.Index + " to " + indexIncludedTo);
                if (logIncludedTo != null && logIncludedFrom != null)
                {
                    //for (var i = LastSnapshotIncludedIndex; i <= indexIncludedTo; i++)
                    stateMachine.ApplyLogsToStateMachine(GetLogRange(LastSnapshotIncludedIndex + 1, indexIncludedTo));
                    SetLastSnapshot(stateMachine.CurrentState, indexIncludedTo, logIncludedTo.Term);
                    // Deleting logs from the index
                    DeleteLogsFromIndex(1, indexIncludedTo - trailingLogCount < 0 ? 1 : indexIncludedTo - trailingLogCount);
                }
            }
        }

        /// <summary>
        /// Locking operation
        /// </summary>
        /// <param name="startIndex">Start index to send from inclusive</param>
        /// <param name="endIndex">End index to send to exclusive</param>
        /// <returns></returns>
        public List<LogEntry> GetLogRange(int startIndexToSendFrom, int endIndexToSendTo)
        {
            lock (_locker)
            {
                List<LogEntry> listToSend = new List<LogEntry>();
                if (startIndexToSendFrom == 0)
                {
                    startIndexToSendFrom = 1;
                }
                for (var i = startIndexToSendFrom; i <= endIndexToSendTo; i++)
                {
                    listToSend.Add(Logs[i]);
                }
                return listToSend;
            }
        }

        public int GetTotalLogCount()
        {
            if (Logs.Count() > 0)
                if (Logs.Last().Key > LastSnapshotIncludedIndex)
                    return Logs.Last().Key;
            return LastSnapshotIncludedIndex;
        }

        public int GetLastLogTerm()
        {
            if (Logs.Count() == 0 && LastSnapshot == null)
            {
                return 0;
            }

            if (Logs.Count() == 0)
            {
                return LastSnapshotIncludedTerm;
            }
            lock (_locker)
            {
                var lastLog = Logs.LastOrDefault();
                return lastLog.Value.Term;
            }
        }

        public int GetLastLogIndex()
        {
            if (Logs.Count() == 0 && LastSnapshot == null)
            {
                return 0;
            }

            if (Logs.Count() == 0)
            {
                return LastSnapshotIncludedIndex;
            }
            lock (_locker)
            {
                var lastLog = Logs.LastOrDefault();
                return lastLog.Value.Index;
            }
        }

        public void SetVotedFor(Guid? id)
        {
            if (id != VotedFor)
            {
                VotedFor = id;
                Save();
            }
        }

        public void SetCurrentTerm(int newTerm)
        {
            if (newTerm != CurrentTerm)
            {
                CurrentTerm = newTerm;
                Save();
            }
        }

        public LogEntry GetLogAtIndex(int logIndex)
        {
            if (logIndex == 0 || GetTotalLogCount() < logIndex || logIndex <= LastSnapshotIncludedIndex || !Logs.ContainsKey(logIndex))
            {
                return null;
            }
            var log = Logs[logIndex];
            if (log.Index == logIndex)
            {
                return log;
            }
            else
            {
                if (Logs.ContainsKey(logIndex))
                {
                    return Logs[logIndex];
                }
                else if (LastSnapshotIncludedIndex >= logIndex)
                {
                    throw new LogSnapshottedException("Log " + logIndex + " has been removed as apart of snapshotting.");
                }
                throw new MissingLogEntryException("Log " + logIndex + " not found in storage.");
            }
        }

        public bool LogExists(int logIndex)
        {
            return Logs.ContainsKey(logIndex);
        }

        public int AddCommands(BaseCommand[] commands, int term)
        {
            if (commands.Length > 0)
            {
                return EnqueueCommand(commands);
            }
            throw new Exception("Weird, I was sent a empty list of base commands");
        }

        public void AddLog(LogEntry entry)
        {
            int index;
            lock (_locker)
            {
                //The entry should be the next log required
                if (entry.Index == GetTotalLogCount() + 1)
                {
                    Logs.Add(entry.Index, entry);
                }
                else if (entry.Index > GetTotalLogCount() + 1)
                {
                    throw new MissingLogEntryException("Something has gone wrong with the concurrency of adding the logs! Failed to add entry " + entry.Index + " as the current index is " + GetTotalLogCount());
                }
            }
            _currentActiveLog = entry.Index;
            Save();
        }

        public void AddLogs(List<LogEntry> entries)
        {
            int index;
            lock (_locker)
            {
                foreach (var entry in entries)
                {
                    //The entry should be the next log required
                    if (entry.Index == GetTotalLogCount() + 1 || entry.Index == (LastSnapshotIncludedIndex + 1))
                    {
                        Logs.Add(entry.Index, entry);
                    }
                    else if (entry.Index > GetTotalLogCount() + 1)
                    {
                        throw new Exception("Something has gone wrong with the concurrency of adding the logs!");
                    }
                }
            }

            _currentActiveLog = entries.Last().Index;
            Save();
        }

        /* public void SetVotedFor(Guid candidateId)
         {
             if (VotedFor != candidateId)
             {
                 VotedFor = candidateId;
                 Save();
             }
         }*/

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fromIndex"></param>
        /// <param name="toIndex">If null it will delete all indexes from the fromIndex to the to index</param>
        public void DeleteLogsFromIndex(int fromIndex, int? toIndex = null)
        {
            lock (_locker)
            {
                List<int> markedForDeletion = new List<int>();
                foreach (var pos in Logs.Where(l => fromIndex <= l.Key && (toIndex == null || l.Key <= toIndex)))
                {
                    markedForDeletion.Add(pos.Key);
                }

                foreach (var delete in markedForDeletion)
                {
                    Logs.Remove(delete);
                }
                Save();
            }
        }

        public void Save()
        {
            RequireSave = true;
        }

        public async Task<bool> SaveThread()
        {
            while (true)
            {
                if (RequireSave)
                {
                    RequireSave = false;
                    await _repository.SaveNodeDataAsync(this);
                }
                else
                {
                    await Task.Delay(500);
                }
            }
        }
    }
}
