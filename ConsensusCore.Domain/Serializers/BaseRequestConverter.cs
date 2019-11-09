using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.RPCs;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsensusCore.Domain.Serializers
{
    public class BaseRequestConverter: JsonCreationConverter<BaseRequest>
    {
        public static BaseState StateType;
        protected override BaseRequest Create(Type objectType, Newtonsoft.Json.Linq.JObject jObject)
        {
            DateTime test = DateTime.Now;
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
