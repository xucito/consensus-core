using ConsensusCore.Node.Controllers;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Utility
{
    //https://stackoverflow.com/questions/36680933/discovering-generic-controllers-in-asp-net-core
    public class NodeControllerApplicationPart : ApplicationPart, IApplicationPartTypeProvider
    {
        public NodeControllerApplicationPart(Type[] entityTypes)
        {
            Types = new List<TypeInfo>() { typeof(NodeController<>).MakeGenericType(entityTypes).GetTypeInfo() };
        }

        public override string Name => "GenericController";
        public IEnumerable<TypeInfo> Types { get; }
    }
}
