using ConsensusCore.Node.Models;
using ConsensusCore.Node.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.BaseClasses
{
    [JsonConverter(typeof(CommandConverter))]
    public abstract class BaseCommand
    {
        public BaseCommand() { }

        public abstract string CommandName { get; }
    }
}
