using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class ShardOperation
    {
        public Guid ShardId { get; set; }
        /// <summary>
        /// Position of the operation
        /// </summary>
        public int Pos { get; set; }
        public ShardOperationOptions Operation { get; set; }
        public Guid ObjectId { get; set; }
        public bool Applied { get; set; } = false;

        public override bool Equals(Object obj)
        {
            //Check for null and compare run-time types.
            if ((obj == null) || !this.GetType().Equals(obj.GetType()))
            {
                return false;
            }
            else
            {
                ShardOperation p = (ShardOperation)obj;
                return (ShardId == p.ShardId) && (Pos == p.Pos) && (Operation == p.Operation) && ObjectId == p.ObjectId && Applied == p.Applied;
            }
        }
    }
}
