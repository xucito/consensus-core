using ConsensusCore.Domain.Serializers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.BaseClasses
{
    [Serializable]
    [JsonConverter(typeof(CommandConverter))]
    public abstract class BaseCommand
    {
        public BaseCommand() { }
        public string DebugLog { get; set; }

        public abstract string CommandName { get; }
    }
}
