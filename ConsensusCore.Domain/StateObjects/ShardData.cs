using ConsensusCore.Domain.Serializers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{

    [JsonConverter(typeof(ShardDataConverter))]
    public abstract class ShardData
    {
        public Guid Id { get; set; }
        /// <summary>
        /// Corresponds to an index
        /// </summary>
        public string _index { get; set; }
        /// <summary>
        /// Corresponds to a shardId
        /// </summary>
        public string _routing { get; set; }
        public override bool Equals(object obj)
        {
            if (obj is ShardData)
            {
                return Id == ((ShardData)obj).Id;
            }
            return false;
        }

        public string GetLockName()
        {
            return "_object:" + Id;
        }
    }
}
