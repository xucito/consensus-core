using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ConsensusCore.Domain.BaseClasses
{
    public class LocalShardMetaData
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        public ConcurrentDictionary<int, ShardOperation> ShardOperations { get; set; } = new ConcurrentDictionary<int, ShardOperation>();
        public ConcurrentDictionary<Guid, DateTime> ObjectsMarkedForDeletion { get; set; } = new ConcurrentDictionary<Guid, DateTime>();
        public object UpdatePositionLock = new object();
        public int LatestShardOperation { get { return ShardOperations.Count; } }
        public int IsUpdating = 0;

        /// <summary>
        /// Upto what point is this shard synced
        /// </summary>
        public int SyncPos = 0;
        public object SysPosLock = new object();
        /*
        {
            get
            {
                var operations = ShardOperations.Where(so => so.Value.Applied);
                if (operations.Count() == 0)
                {
                    return 0;
                }
                else
                {
                    return ShardOperations.Where(so => so.Value.Applied).Last().Key;
                }
            }
        }*/

        private object shardOperationsLock = new object();
        public int AddShardOperation(ShardOperation operation)
        {
            int noOfShardOperations;
            lock (shardOperationsLock)
            {
                noOfShardOperations = ShardOperations.Count + 1;
                bool addResult = ShardOperations.TryAdd(noOfShardOperations, operation);
                if (!addResult)
                {
                    Console.WriteLine("ERROR:Failed to add operation!!!");
                    throw new Exception("Failed to add shard operation to disk.");
                }
            }

            return noOfShardOperations;
        }

        public void MarkShardAsApplied(int pos)
        {
            ShardOperations[pos].Applied = true;
            lock (SysPosLock)
            {
                if (pos > SyncPos)
                {
                    //Console.WriteLine("Updated sync position with " + pos + "from sync pos" + SyncPos);
                    SyncPos = pos;
                }
            }
        }

        /*public void UpdateSyncPosition(int uptoPosition)
        {
            while (SyncPos < uptoPosition - 1)
            {
                Console.WriteLine("Waiting for data to sync to " + uptoPosition);
                Thread.Sleep(100);
            }

            bool LogExists = ShardOperations.TryGetValue(SyncPos + 1, out _);
            bool LogApplied = LogExists ? ShardOperations[SyncPos + 1].Applied : false;

            if (LogExists && LogApplied)
            {
                SyncPos++;
            }
            else
            {
                Console.WriteLine("CRITICAL ERROR UPDATING POSITION");
            }
            /*
            lock (UpdatePositionLock)
            {
                //Update only based on applied operations
                while (SyncPos < uptoPosition)
                {
                    bool LogExists = ShardOperations.TryGetValue(SyncPos + 1, out _);
                    bool LogApplied = LogExists ? ShardOperations[SyncPos + 1].Applied : false;

                    if (LogExists && LogApplied)
                    {
                        SyncPos++;
                    }
                    else
                    {
                        Console.WriteLine("Waiting for sync for shard " + ShardId + " upto " + uptoPosition + " current sync position is " + SyncPos + (!LogExists ? "Log does not exists" : "Log is not applied"));
                        Thread.Sleep(100);
                    }
                }
            }
        }*/

        public bool CanApplyOperation(int pos)
        {
            if (pos == 1)
            {
                return true;
            }

            if (!ShardOperations.ContainsKey(pos - 1) || !ShardOperations[pos - 1].Applied)
            {
                return false;
            }
            return true;
        }

        public bool ReplicateShardOperation(int pos, ShardOperation operation)
        {
            bool successfullyAddedOperation = false;

            lock (shardOperationsLock)
            {
                successfullyAddedOperation = ShardOperations.TryAdd(pos, operation);
            }
            return successfullyAddedOperation;
        }


        /// <summary>
        /// Only used when the operation was recorded but not applied to datastore
        /// </summary>
        /// <returns></returns>
        public bool RemoveOperation(int pos)
        {
            if (SyncPos < pos)
            {
                return ShardOperations.TryRemove(pos, out _);
            }
            throw new Exception("CONCURRENCY ERROR while trying to reverse operation, the sync position is " + SyncPos + " and the position to remove is " + pos +".");
        }

        public bool MarkObjectForDeletion(Guid objectId)
        {
            return ObjectsMarkedForDeletion.TryAdd(objectId, DateTime.Now);
        }


        public bool ObjectIsMarkedForDeletion(Guid objectId)
        {
            return ObjectsMarkedForDeletion.TryGetValue(objectId, out _);
        }
    }
}
