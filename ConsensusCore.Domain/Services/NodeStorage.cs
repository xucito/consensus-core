using ConsensusCore.Domain.BaseClasses;
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
    public class NodeStorage
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public double Version { get; set; } = 1.0;
        public int CurrentTerm { get; set; } = 0;
        public Guid? VotedFor { get; set; } = null;
        public List<LogEntry> Logs { get; set; } = new List<LogEntry>();
        [JsonIgnore]
        public readonly object _locker = new object();
        [JsonIgnore]
        private IBaseRepository _repository;
        public ConcurrentDictionary<Guid, LocalShardMetaData> ShardMetaData { get; set; } = new ConcurrentDictionary<Guid, LocalShardMetaData>();
        [JsonIgnore]
        public bool RequireSave = false;
        [JsonIgnore]
        public readonly object _saveLocker = new object();
        [JsonIgnore]
        public Thread _saveThread;
        /*public NodeStorage()
         {
         }
         */

        public NodeStorage(IBaseRepository repository)
        {
            Id = Guid.NewGuid();
            _repository = repository;
            if (ShardMetaData == null)
            {
                ShardMetaData = new ConcurrentDictionary<Guid, LocalShardMetaData>();
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

        public void SetRepository(IBaseRepository repository)
        {
            _repository = repository;
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

        public int GetLogCount()
        {
            return Logs.Count();
        }

        public int GetLastLogTerm()
        {
            var lastLog = Logs.LastOrDefault();
            if (lastLog != null)
            {
                return lastLog.Term;
            }
            return 0;
        }

        public int GetLastLogIndex()
        {
            var lastLog = Logs.LastOrDefault();
            if (lastLog != null)
            {
                return lastLog.Index;
            }
            return 0;
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
                return Logs.Where(l => l.Index == logIndex).FirstOrDefault();
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
                    Logs.Add(new LogEntry()
                    {
                        Commands = commands,
                        Term = term,
                        Index = index
                    });
                }

                if (_repository != null)
                {
                    //Console.WriteLine("Add commands");
                    Save();
                }
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

                    //Console.WriteLine("Add logs");
                    Logs.Add(entry);
                    Save();
                }
                else if (entry.Index > Logs.Count() + 1)
                {
                    throw new Exception("Something has gone wrong with the concurrency of adding the logs!");
                }
            }

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
                Logs.RemoveRange(index - 1, Logs.Count() - index + 1);

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
                Thread.Sleep(500);
                if (RequireSave)
                {
                    RequireSave = false;
                    _repository.SaveNodeData(this);
                }
            }
        }
    }
}
