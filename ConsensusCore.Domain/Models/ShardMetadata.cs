using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    /// <summary>
    /// Local copy of the data on the node
    /// </summary>
    public class ShardMetadata
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
    }
}
