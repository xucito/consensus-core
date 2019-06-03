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

        private object shardOperationsLock = new object();
        public int AddShardOperation(ShardOperation operation)
        {
            lock (shardOperationsLock)
            {
                var noOfShardOperations = ShardOperations.Count;
                ShardOperations.Add(noOfShardOperations, operation);
                return noOfShardOperations;
            }
        }

        public void ReplicateShardOperation(int pos, ShardOperation operation)
        {
            lock (shardOperationsLock)
            {
                ShardOperations.Add(pos, operation);
            }
        }
    }
}
