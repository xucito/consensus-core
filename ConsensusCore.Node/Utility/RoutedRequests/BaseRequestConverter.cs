using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsensusCore.Node.Utility
{
    public class BaseRequestConverter : JsonCreationConverter<BaseRequest>
    {
        protected override BaseRequest Create(Type objectType, Newtonsoft.Json.Linq.JObject jObject)
        {
            foreach (Type t in Assembly.GetExecutingAssembly().GetTypes())
            {
                if (t.IsSubclassOf(typeof(BaseRequest)))
                {
                    if (!t.IsGenericTypeDefinition && jObject.Value<string>("RequestName") == ((BaseRequest)Activator.CreateInstance(t)).RequestName)
                    {
                        return (BaseRequest)Activator.CreateInstance(t);
                    }
                }
            }

            throw new Exception("Routed request configuration was not found");
        }
    }
}
