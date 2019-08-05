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
        /// <summary>
        /// This should not be duplicated when adding
        /// </summary>
        public Guid Id { get; set; }
        /// <summary>
        /// This Id can be used to identifier duplicates in running tasks
        /// </summary>
        public string UniqueRunningId { get; set; }
        public string Description { get; set; }
        public Guid NodeId { get; set; }
        public abstract string Name { get; }
        public ClusterTaskStatuses Status { get; set; }
        public DateTime CreatedOn { get; set; }
        public DateTime? CompletedOn { get; set; }
        public string ErrorMessage { get; set; }

        public override string ToString()
        {
            return "Task:" + Id + UniqueRunningId;
        }
    }
}
