using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public class ShardData
    {
        public Guid Id { get; set; }
        public Guid? ShardId { get; set; }
        public string Type { get; set; }
        public object Data { get; set; }
        /// <summary>
        /// Used to track what number of the record this is
        /// </summary>
        public int Position { get; set; }
    }
}
