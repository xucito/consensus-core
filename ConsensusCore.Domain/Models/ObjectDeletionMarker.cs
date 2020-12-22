using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    /// <summary>
    /// Object used to mark an object to be deleted
    /// </summary>
    public class ObjectDeletionMarker
    {
        //Pos of the original deletion record
        public int Pos { get; set; }
        public Guid Id { get; set; }
        public Guid ShardId { get; set; }
        public List<string> ShardWriteOperationIds { get; set; }
    }
}
