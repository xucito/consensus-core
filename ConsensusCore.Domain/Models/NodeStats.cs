using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    public class NodeStats
    {
        public int OperationsInQueue { get; set; }
        public int OperationsInTransit { get; set; }
    }
}
