using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class ShardWriteOperation
    {
        /// <summary>
        /// Unique Transaction Id
        /// </summary>
        public string Id { get; set; }
        /// <summary>
        /// Position of the operation
        /// </summary>
        public int Pos { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public ShardData Data { get; set; }
        /// <summary>
        /// The hash of all transactions upto this point
        /// </summary>
        public string ShardHash { get; set; }
        /// <summary>
        /// Date the transaction was queued
        /// </summary>
        public DateTime TransactionDate { get; set; }

        public bool Applied { get; set; }
        
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            ShardWriteOperation objAsPart = obj as ShardWriteOperation;
            if (objAsPart == null) return false;
            else return Equals(objAsPart);
        }

        public bool Equals(ShardWriteOperation operation)
        {
            return operation.Id == Id;
        }

        public WriteConsistencyLevels ConsistencyLevel { get; set; } = WriteConsistencyLevels.Queued;
    }
}
