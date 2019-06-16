using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class LocalShardMetaData
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        public ConcurrentDictionary<int, ShardOperation> ShardOperations { get; set; }
        /// <summary>
        /// Upto what point is this shard synced
        /// </summary>
        public int SyncPos { get; set; } = 0;

        private object shardOperationsLock = new object();
        public int AddShardOperation(ShardOperation operation)
        {
            int noOfShardOperations;
            lock (shardOperationsLock)
            {
                noOfShardOperations = ShardOperations.Count + 1;
                ShardOperations.TryAdd(noOfShardOperations, operation);
                // Try get the next value
                while (ShardOperations.TryGetValue(SyncPos + 1, out _))
                {
                    SyncPos++;
                }
            }

            return noOfShardOperations;
        }

        public bool ReplicateShardOperation(int pos, ShardOperation operation)
        {
            bool successfullyAddedOperation = false;
            lock (shardOperationsLock)
            {
                successfullyAddedOperation = ShardOperations.TryAdd(pos, operation);
                //If the value was updated, then try to increment syncPos
                if (successfullyAddedOperation)
                    // Try get the next value
                    while (ShardOperations.TryGetValue(SyncPos + 1, out _))
                    {
                        SyncPos++;
                    }
            }
            return successfullyAddedOperation;
        }
    }
}
