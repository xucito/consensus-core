using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class ShardWriteOperationLog
    {
        public string Id { get; set; }
        /// <summary>
        /// Position of the operation
        /// </summary>
        public int Pos { get; set; }
        /// <summary>
        /// The Unique id of the object this operation contains
        /// </summary>
        public Guid ObjectId { get; set; }
        /// <summary>
        /// Type of operation
        /// </summary>
        public ShardOperationOptions Operation { get; set; }
        /// <summary>
        /// Compressed shard data
        /// </summary>
        public byte[] Data { get; set; }
        /// <summary>
        /// The hash of all transactions upto this point
        /// </summary>
        public string ShardHash { get; set; }
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            ShardWriteOperationLog objAsPart = obj as ShardWriteOperationLog;
            if (objAsPart == null) return false;
            else return Equals(objAsPart);
        }

        public bool Equals(ShardWriteOperationLog operation)
        {
            return operation.Id == Id;
        }
    }
}
