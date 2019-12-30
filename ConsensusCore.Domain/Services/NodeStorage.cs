using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;

namespace ConsensusCore.Domain.Services
{
    public class NodeStorage<State> : INodeStorage<State> where State : BaseState, new()
    {
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
        public object _locker = new object();
        [JsonIgnore]
        private IBaseRepository<State> _repository;
        [JsonIgnore]
        public bool RequireSave = false;
        [JsonIgnore]
        public readonly object _saveLocker = new object();
        [JsonIgnore]
        public Thread _saveThread;

        public NodeStorage()
        {
        }


        public NodeStorage(IBaseRepository<State> repository)
        {
            _repository = repository;

            var loadedData = repository.LoadNodeData();
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
                Console.WriteLine("Failed to load local node storage from store, creating new node storage");
                Save();
            }

            if (_repository != null)
            {
                _saveThread = new Thread(() =>
                {
                    SaveThread();
                });
                _saveThread.Start();
            }

            // var loadedData = _repository.LoadNodeData();
        }

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

        public void CreateSnapshot(int indexIncludedTo)
        {
            var stateMachine = new StateMachine<State>();

            if(!LogExists(LastSnapshotIncludedIndex + 1))
            {
                return;
            }

            //Find the log position of the commit index
            if (LastSnapshot != null)
            {
                stateMachine.ApplySnapshotToStateMachine(LastSnapshot);
            }

            var logIncludedTo = GetLogAtIndex(indexIncludedTo);
            var logIncludedFrom = GetLogAtIndex(LastSnapshotIncludedIndex + 1);
            Console.WriteLine("Getting logs " + logIncludedFrom.Index + " to " + logIncludedTo);
            if (logIncludedTo != null && logIncludedFrom != null)
            {
                //for (var i = LastSnapshotIncludedIndex; i <= indexIncludedTo; i++)
                stateMachine.ApplyLogsToStateMachine(GetLogRange(LastSnapshotIncludedIndex + 1, indexIncludedTo));

                LastSnapshot = stateMachine.CurrentState;
                LastSnapshotIncludedIndex = indexIncludedTo;
                LastSnapshotIncludedTerm = logIncludedTo.Term;

                lock (_locker)
                {
                    // Deleting logs from the index
                    DeleteLogsFromIndex(1, indexIncludedTo);
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
            var lastLog = Logs.LastOrDefault();
            return lastLog.Value.Term;
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
            var lastLog = Logs.LastOrDefault();
            return lastLog.Value.Index;
        }

        public void SetVotedFor(Guid? id)
        {
            if (id != VotedFor)
            {
                VotedFor = id;


                //Console.WriteLine("Set voted for");
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

        public int AddCommands(List<BaseCommand> commands, int term)
        {
            if (commands.Count > 0)
            {
                int index;
                lock (_locker)
                {
                    index = GetTotalLogCount() + 1;
                    Logs.Add(index, new LogEntry()
                    {
                        Commands = commands,
                        Term = term,
                        Index = index
                    });
                }
                //Console.WriteLine("Add commands");
                Save();
                return index;
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
                    if (entry.Index == GetTotalLogCount() + 1 || entry.Index == LastSnapshotIncludedIndex + 1)
                    {
                        Logs.Add(entry.Index, entry);
                    }
                    else if (entry.Index > GetTotalLogCount() + 1)
                    {
                        throw new Exception("Something has gone wrong with the concurrency of adding the logs!");
                    }
                }
            }
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

                //Console.WriteLine("Delete logs from index");
                Save();
            }
        }

        public void Save()
        {
            RequireSave = true;
        }

        public void SaveThread()
        {
            while (true)
            {
                if (RequireSave)
                {
                    RequireSave = false;
                    _repository.SaveNodeData(this);
                }
                else
                {
                    Thread.Sleep(500);
                }
            }
        }
    }
}
