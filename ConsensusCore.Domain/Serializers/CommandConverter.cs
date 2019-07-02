using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsensusCore.Domain.Serializers
{
    public class CommandConverter : JsonCreationConverter<BaseCommand>
    {
        protected override BaseCommand Create(Type objectType, Newtonsoft.Json.Linq.JObject jObject)
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (Type t in assembly.GetTypes())
                {
                    if (t.IsSubclassOf(typeof(BaseCommand)))
                    {
                        if (jObject.Value<string>("CommandName") == ((BaseCommand)Activator.CreateInstance(t)).CommandName)
                        {
                            var instance = (BaseCommand)Activator.CreateInstance(t);
                            return instance;
                        }
                    }
                }
            }

            return new DefaultCommand();
        }
    }
}
