using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsensusCore.Node.Utility
{
    public class CommandConverter: JsonCreationConverter<BaseCommand>
    {
        protected override BaseCommand Create(Type objectType, Newtonsoft.Json.Linq.JObject jObject)
        {
            foreach (Type t in Assembly.GetExecutingAssembly().GetTypes())
            {
                if (t.IsSubclassOf(typeof(BaseCommand)))
                {
                    if (jObject.Value<string>("CommandName") == ((BaseCommand)Activator.CreateInstance(t)).CommandName)
                    {
                        return (BaseCommand)Activator.CreateInstance(t);
                    }
                }
            }

            return new DefaultCommand();
        }
    }
}
