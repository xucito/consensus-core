using ConsensusCore.Node.Utility.RoutedRequests;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{

    [JsonConverter(typeof(ShardDataConverter))]
    public abstract class ShardData
    {
        public Guid Id { get; set; }
        public Guid? ShardId { get; set; }
        public string Type { get; set; }
        public object Data { get; set; }
        public string ClassName { get { return this.GetType().FullName; } }
    }
}
