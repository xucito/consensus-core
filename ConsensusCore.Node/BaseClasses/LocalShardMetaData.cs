using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class LocalShardMetaData
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        public SortedList<int, ShardOperation> ShardOperations { get; set; }
        /// <summary>
        /// Upto what point is this shard synced
        /// </summary>
        public int SyncPos { get; set; }

        private object shardOperationsLock = new object();
        public int AddShardOperation(ShardOperation operation)
        {
            int noOfShardOperations;
            lock (shardOperationsLock)
            {
                noOfShardOperations = ShardOperations.Count;
                ShardOperations.Add(noOfShardOperations, operation);
                // Try get the next value
                while (ShardOperations.TryGetValue(SyncPos + 1, out _))
                {
                    SyncPos++;
                }
            }

            return noOfShardOperations;
        }

        public void ReplicateShardOperation(int pos, ShardOperation operation)
        {
            lock (shardOperationsLock)
            {
                ShardOperations.Add(pos, operation);
                // Try get the next value
                while (ShardOperations.TryGetValue(SyncPos + 1, out _))
                {
                    SyncPos++;
                }
            }
        }
    }
}
