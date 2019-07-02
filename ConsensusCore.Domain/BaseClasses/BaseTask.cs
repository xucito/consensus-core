using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Serializers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    [JsonConverter(typeof(BaseClusterTaskConverter))]
    public abstract class BaseTask
    {
        public Guid NodeId { get; set; }
        public abstract string Name { get; }
        public ClusterTaskStatuses Status { get; set; }
        public DateTime CreatedOn { get; set; }
        public DateTime? CompletedOn { get; set; }
    }
}
