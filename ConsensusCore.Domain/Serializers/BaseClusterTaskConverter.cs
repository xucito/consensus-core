using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsensusCore.Domain.Serializers
{
    public class BaseClusterTaskConverter : JsonCreationConverter<BaseTask>
    {
        protected override BaseTask Create(Type objectType, Newtonsoft.Json.Linq.JObject jObject)
        {
            DateTime test = DateTime.Now;
            foreach (Type t in Assembly.GetExecutingAssembly().GetTypes())
            {
                if (t.IsSubclassOf(typeof(BaseTask)))
                {
                    if (!t.IsGenericTypeDefinition && jObject.Value<string>("Name") == ((BaseTask)Activator.CreateInstance(t)).Name)
                    {
                        return (BaseTask)Activator.CreateInstance(t);
                    }
                }
            }

            throw new Exception("Routed request configuration was not found");
        }
    }
}
