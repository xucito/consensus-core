using ConsensusCore.Node.BaseClasses;
using Newtonsoft.Json.Linq;
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
            foreach (Type t in Assembly.GetEntryAssembly().GetTypes())
            {
                if (t.IsSubclassOf(typeof(ShardData)))
                {
                    //Class insensitive
                    string className = jObject.GetValue("className", StringComparison.OrdinalIgnoreCase).Value<string>();
                    if (!t.IsGenericTypeDefinition && className == ((ShardData)Activator.CreateInstance(t)).ClassName)
                    {
                        return (ShardData)Activator.CreateInstance(t);
                    }
                }
            }

            throw new Exception("Routed request configuration was not found");
        }
    }
}
