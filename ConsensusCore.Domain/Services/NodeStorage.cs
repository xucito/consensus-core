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
    public class NodeStorage<Z> where Z : BaseState, new()
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
        public Z LastSnapshot { get; set; }
        public SortedList<int, LogEntry> Logs { get; set; } = new SortedList<int, LogEntry>();
        public ConcurrentBag<DataReversionRecord> RevertedOperations = new ConcurrentBag<DataReversionRecord>();
        [JsonIgnore]
        public object _locker = new object();
        [JsonIgnore]
        private IBaseRepository _repository;
        public ConcurrentDictionary<Guid, LocalShardMetaData> ShardMetaData { get; set; } = new ConcurrentDictionary<Guid, LocalShardMetaData>();
        [JsonIgnore]
        public bool RequireSave = false;
        [JsonIgnore]
        public readonly object _saveLocker = new object();
        [JsonIgnore]
        public Thread _saveThread;

        public NodeStorage()
        {
        }


        public NodeStorage(IBaseRepository repository)
        {
            _repository = repository;
            if (ShardMetaData == null)
            {
                ShardMetaData = new ConcurrentDictionary<Guid, LocalShardMetaData>();
            }


            var loadedData = repository.LoadNodeData<Z>();
            if (loadedData != null)
            {
                Id = loadedData.Id;
                Name = loadedData.Name;
                Version = loadedData.Version;
                CurrentTerm = loadedData.CurrentTerm;
                VotedFor = loadedData.VotedFor;
                Logs = loadedData.Logs;
                ShardMetaData = loadedData.ShardMetaData;

                LastSnapshot = loadedData.LastSnapshot;
                LastSnapshotIncludedTerm = loadedData.LastSnapshotIncludedTerm;
                LastSnapshotIncludedIndex = loadedData.LastSnapshotIncludedIndex;
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

        public void AddNewShardMetaData(LocalShardMetaData metadata)
        {
            if (!ShardMetaData.TryAdd(metadata.ShardId, metadata))
            {
                throw new Exception("Failed to add new shard metadata");
            }
            //Console.WriteLine("Added new shard metadata");
            Save();
        }

        public void CreateSnapshot(int currentCommitIndex)
        {
            var stateMachine = new StateMachine<Z>();
            //Find the log position of the commit index
            if (LastSnapshot != null)
            {
                stateMachine.ApplySnapshotToStateMachine(LastSnapshot);
            }

            for (var i = LastSnapshotIncludedIndex; i <= currentCommitIndex; i++)
                stateMachine.ApplyLogsToStateMachine(GetLogRange(LastSnapshotIncludedIndex + 1, currentCommitIndex));

            // Deleting logs from the index
            DeleteLogsFromIndex(currentCommitIndex);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startIndex">Start index to send from inclusive</param>
        /// <param name="endIndex">End index to send to exclusive</param>
        /// <returns></returns>
        public List<LogEntry> GetLogRange(int startIndexToSendFrom, int endIndexToSendTo)
        {
            List<LogEntry> listToSend = new List<LogEntry>();
            for (var i = startIndexToSendFrom; i <= endIndexToSendTo; i++)
            {
                listToSend.Add(Logs[i]);
            }
            return listToSend;
        }

        /// <summary>
        /// When you are adding a new shard, the index is created
        /// </summary>
        public int AddNewShardOperation(Guid shardId, ShardOperation operation)
        {
            var result = ShardMetaData[shardId].AddShardOperation(operation);

            //Console.WriteLine("Added new shard operation");
            Save();
            return result;
        }

        public bool ReplicateShardOperation(Guid shardId, int pos, ShardOperation operation)
        {
            var result = ShardMetaData[shardId].ReplicateShardOperation(pos, operation);
            if (result)
            {
                //Console.WriteLine("Replicate shard operation");
                Save();
            }
            return result;
        }

        public bool CanApplyOperation(Guid shardId, int pos)
        {
            return ShardMetaData[shardId].CanApplyOperation(pos);
        }

        public void MarkOperationAsCommited(Guid shardId, int pos)
        {
            ShardMetaData[shardId].MarkShardAsApplied(pos);

            //Console.WriteLine("MarkOperationAsCommited");
            Save();
            // ShardMetaData[shardId].UpdateSyncPosition(pos);
        }

        public bool RemoveOperation(Guid shardId, int pos)
        {
            var result = ShardMetaData[shardId].RemoveOperation(pos);

            //Console.WriteLine("Remove operation");
            Save();
            return result;
        }

        public int GetTotalLogCount()
        {
            return Logs.Last().Key;
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

                //Console.WriteLine("Set term for");
                CurrentTerm = newTerm;
                Save();
            }
        }

        public LogEntry GetLogAtIndex(int logIndex)
        {
            if (logIndex == 0 || Logs.Count() < logIndex)
            {
                return null;
            }
            var log = Logs[logIndex - 1];
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
                    throw new LogSnapshottedException();
                }
                throw new Exception("Log " + logIndex + " not found in storage.");
            }
        }

        public int AddCommands(List<BaseCommand> commands, int term)
        {
            if (commands.Count > 0)
            {
                int index;
                lock (_locker)
                {
                    index = Logs.Count() + 1;
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
                if (entry.Index == Logs.Count() + 1)
                {
                    Logs.Add(entry.Index, entry);
                }
                else if (entry.Index > Logs.Count() + 1)
                {
                    throw new Exception("Something has gone wrong with the concurrency of adding the logs!");
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

        public void DeleteLogsFromIndex(int index)
        {
            lock (_locker)
            {
                List<int> markedForDeletion = new List<int>();
                foreach (var pos in Logs)
                {
                    if (pos.Key >= index)
                    {
                        markedForDeletion.Add(pos.Key);
                    }
                    else
                    {
                        break;
                    }
                }
                foreach (var delete in markedForDeletion)
                {
                    Logs.Remove(delete);
                }

                //Console.WriteLine("Delete logs from index");
                Save();
            }
        }

        public int GetCurrentShardPos(Guid shardId)
        {
            return ShardMetaData[shardId].SyncPos;
        }

        public int GetCurrentShardLatestCount(Guid shardId)
        {
            return ShardMetaData[shardId].ShardOperations.Count();
        }

        public LocalShardMetaData GetShardMetadata(Guid shardId)
        {
            if (ShardMetaData.ContainsKey(shardId))
                return ShardMetaData[shardId];
            return null;
        }

        public void UpdateShardSyncPosition(Guid shardId, int syncPos)
        {
            ShardMetaData[shardId].SyncPos = syncPos;
        }

        public ShardOperation GetOperation(Guid shardId, int pos)
        {
            if (ShardMetaData.ContainsKey(shardId) && ShardMetaData[shardId].ShardOperations.ContainsKey(pos))
                return ShardMetaData[shardId].ShardOperations[pos];
            return null;
        }

        public bool MarkShardForDeletion(Guid shardId, Guid objectId)
        {
            var result = false;
            if (ShardMetaData.ContainsKey(shardId))
            {
                result = ShardMetaData[shardId].MarkObjectForDeletion(objectId);

                //Console.WriteLine("marked shard for deletion");
                Save();
            }
            return result;
        }

        public bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId)
        {
            if (ShardMetaData.ContainsKey(shardId))
            {
                return ShardMetaData[shardId].ObjectIsMarkedForDeletion(objectId);
            }
            return false;
        }

        public Dictionary<Guid, int> GetShardSyncPositions()
        {
            if (ShardMetaData == null)
            {
                ShardMetaData = new ConcurrentDictionary<Guid, LocalShardMetaData>();
            }
            var shardMetadata = ShardMetaData;
            return shardMetadata.ToDictionary(k => k.Key, v => v.Value.ShardOperations.Count);
        }
        public Dictionary<Guid, int> GetShardOperationCounts()
        {
            if (ShardMetaData == null)
            {
                ShardMetaData = new ConcurrentDictionary<Guid, LocalShardMetaData>();
            }
            return ShardMetaData.ToDictionary(k => k.Key, v => v.Value.ShardOperations.Count);
        }

        public ShardOperation GetShardOperation(Guid shardId, int pos)
        {
            if (ShardMetaData.ContainsKey(shardId) && ShardMetaData[shardId].ShardOperations.ContainsKey(pos))
            {
                return ShardMetaData[shardId].ShardOperations[pos];
            }
            return null;
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

        public void AddDataReversionRecord(DataReversionRecord record)
        {
            RevertedOperations.Add(record);
            Save();
        }
    }
}
