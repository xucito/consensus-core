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
        public Guid? ShardId { get; set; }
        public string ShardType { get; set; }
        public string ClassName { get { return this.GetType().FullName; } }
        // The version of the shard
        public List<string> Versions { get; set; } = new List<string>();
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
            return "_object:" + ShardType + ":" + Id;
        }
    }
}
