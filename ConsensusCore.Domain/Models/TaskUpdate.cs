using ConsensusCore.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    public class TaskUpdate
    {
        public Guid TaskId { get; set; }
        public ClusterTaskStatuses Status { get; set; }
        public DateTime CompletedOn { get; set; }
    }
}
