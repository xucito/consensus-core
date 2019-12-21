using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.ValueObjects
{
    public class MetricType
    {
        /// <summary>
        /// Name of the metric
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// The unit used for the value expressed in the metric
        /// </summary>
        public string Unit { get; set; }
        /// <summary>
        /// How the data is being aggregated avg, type
        /// </summary>
        public string AggregationType { get; set; }
        /// <summary>
        /// Lower context for the metric for additional filtering
        /// </summary>
        public string SubCategory { get; set; }
    }
}
