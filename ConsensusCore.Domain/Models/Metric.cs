using ConsensusCore.Domain.ValueObjects;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    public class Metric
    {
        public MetricType Type {get;set;}
        /// <summary>
        /// Interval measured from the Date + Interval in Ms
        /// </summary>
        public int IntervalMs { get; set; }
        /// <summary>
        /// The start date of the metric
        /// </summary>
        public DateTime Date { get; set; }

        public double Value { get; set; }
    }
}
