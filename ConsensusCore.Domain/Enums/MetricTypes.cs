using ConsensusCore.Domain.ValueObjects;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Enums
{
    public static class MetricTypes
    {
        public static MetricType ClusterCommandElapsed(string subMetric)
        {
            return new MetricType()
            {
                Name = "ClusterCommandElapsed",
                AggregationType = AggregationType.Abs.Value,
                Unit = "ms",
                SubCategory = subMetric
            };
        }
    }
}
