using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsensusCore.Node.Utility.RoutedRequests
{
    public class ShardDataConverter : JsonCreationConverter<ShardData>
    {
        protected override ShardData Create(Type objectType, Newtonsoft.Json.Linq.JObject jObject)
        {
            DateTime test = DateTime.Now;
            foreach (Type t in Assembly.GetEntryAssembly().GetTypes())
            {
                if (t.IsSubclassOf(typeof(ShardData)))
                {
                    if (!t.IsGenericTypeDefinition && jObject.Value<string>("ClassName") == ((ShardData)Activator.CreateInstance(t)).ClassName)
                    {
                        return (ShardData)Activator.CreateInstance(t);
                    }
                }
            }

            throw new Exception("Routed request configuration was not found");
        }
    }
}
