using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public class NodeStorage
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string Name { get; set; }
        public double Version { get; set; } = 1.0;
        public int CurrentTerm { get; set; } = 0;
        public Guid? VotedFor { get; set; } = null;
        public List<LogEntry> Logs { get; set; } = new List<LogEntry>();
        public readonly object _locker = new object();
        private BaseRepository _repository;
        public Dictionary<Guid, LocalShardMetaData> ShardMetaData { get; } = new Dictionary<Guid, LocalShardMetaData>();

        public NodeStorage()
        {
        }

        public void AddNewShardMetaData(LocalShardMetaData metadata)
        {
            ShardMetaData.Add(metadata.ShardId, metadata);
        }

        /// <summary>
        /// When you are adding a new shard, the index is created
        /// </summary>
        public int AddNewShardOperation(Guid shardId, ShardOperation operation)
        {
            return ShardMetaData[shardId].AddShardOperation(operation);
        }

        public bool ReplicateShardOperation(Guid shardId, int pos, ShardOperation operation)
        {
            return ShardMetaData[shardId].ReplicateShardOperation(pos, operation);
        }

        public void MarkOperationAsCommited(Guid shardId, int pos)
        {
            ShardMetaData[shardId].MarkShardAsApplied(pos);
            ShardMetaData[shardId].UpdateSyncPosition(pos);
        }

        public bool RemoveOperation(Guid shardId, int pos)
        {
            return ShardMetaData[shardId].RemoveOperation(pos);
        }

        public NodeStorage(BaseRepository repository)
        {
            _repository = repository;
            var storedData = _repository.LoadNodeData();
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
                _repository.SaveNodeData();

            return index;
        }

        public void AddLog(LogEntry entry)
        {
            int index;
            lock (_locker)
            {
                //The entry should be the next log required
                if (entry.Index == Logs.Count() + 1)
                {
                    Logs.Add(entry);
                }
                else if (entry.Index > Logs.Count() + 1)
                {
                    throw new Exception("Something has gone wrong with the concurrency of adding the logs!");
                }
            }

            if (_repository != null)
                _repository.SaveNodeData();
        }

        public void UpdateCurrentTerm(int newterm)
        {
            CurrentTerm = newterm;

            if (_repository != null)
                _repository.SaveNodeData();
        }

        public void SetVotedFor(Guid candidateId)
        {
            VotedFor = candidateId;
            if (_repository != null)
                _repository.SaveNodeData();
        }

        public void DeleteLogsFromIndex(int index)
        {
            lock (_locker)
            {
                Logs.RemoveRange(index - 1, Logs.Count() - index + 1);
                if (_repository != null)
                    _repository.SaveNodeData();
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

        public ShardOperation GetOperation(Guid shardId, int pos)
        {
            if (ShardMetaData.ContainsKey(shardId) && ShardMetaData[shardId].ShardOperations.ContainsKey(pos))
                return ShardMetaData[shardId].ShardOperations[pos];
            return null;
        }

        public bool MarkShardForDeletion(Guid shardId, Guid objectId)
        {
            if (ShardMetaData.ContainsKey(shardId))
            {
                return ShardMetaData[shardId].MarkObjectForDeletion(objectId);
            }
            return false;
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
            return ShardMetaData.ToDictionary(k => k.Key, v => v.Value.ShardOperations.Count);
        }
        public Dictionary<Guid, int> GetShardOperationCounts()
        {
            return ShardMetaData.ToDictionary(k => k.Key, v => v.Value.ShardOperations.Count);
        }
    }
}
