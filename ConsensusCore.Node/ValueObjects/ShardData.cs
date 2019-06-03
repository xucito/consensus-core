using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.ValueObjects
{
    public class ShardData
    {
        public DataStates State { get; set; }

        /// <summary>
        /// Version of the shard that this data was last added
        /// </summary>
        public int Version { get; set; }
    }
}
