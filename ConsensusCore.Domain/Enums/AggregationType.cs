using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Enums
{
    public class AggregationType
    {
        private AggregationType(string value) { Value = value; }

        public string Value { get; set; }

        public static AggregationType Max { get { return new AggregationType("Max"); } }
        public static AggregationType Average { get { return new AggregationType("Average"); } }
        public static AggregationType Min { get { return new AggregationType("Min"); } }
        public static AggregationType Abs { get { return new AggregationType("Abs"); } }
    }
}
