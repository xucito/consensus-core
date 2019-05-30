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
            DateTime test = DateTime.Now;
            foreach (Type t in Assembly.GetExecutingAssembly().GetTypes())
            {
                if (t.IsSubclassOf(typeof(BaseCommand)))
                {
                    if (jObject.Value<string>("CommandName") == ((BaseCommand)Activator.CreateInstance(t)).CommandName)
                    {
                        var instance = (BaseCommand)Activator.CreateInstance(t);

                        Console.WriteLine("Time to deserialize " + (DateTime.Now - test).TotalMilliseconds);
                        return instance;
                    }
                }
            }

            return new DefaultCommand();
        }
    }
}
