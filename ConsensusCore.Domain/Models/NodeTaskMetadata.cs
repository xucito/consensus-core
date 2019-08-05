using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Models
{
    public class NodeTaskMetadata
    {
        public Guid Id { get; set; }
        public Task Task { get; set; }
    }
}
