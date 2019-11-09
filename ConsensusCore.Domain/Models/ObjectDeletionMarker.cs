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
        public Guid ShardId { get; set; }
        public Guid ObjectId { get; set; }
        public DateTime GeneratedOn { get; set; }
    }
}
