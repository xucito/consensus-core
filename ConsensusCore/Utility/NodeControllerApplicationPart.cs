using ConsensusCore.Controllers;
using ConsensusCore.Interfaces;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace ConsensusCore.Utility
{
    //https://stackoverflow.com/questions/36680933/discovering-generic-controllers-in-asp-net-core
    public class NodeControllerApplicationPart : ApplicationPart, IApplicationPartTypeProvider
    {
        public NodeControllerApplicationPart(Type[] entityTypes)
        {
            /* TypeInfo[] closedControllerTypes = entityTypes
                 .Select(et => typeof(NodeController<,,>).MakeGenericType(entityTypes))
                 .Select(cct => cct.GetTypeInfo())
                 .ToArray();
                 */
            var ammendedEntityTypes = entityTypes.ToList();
            ammendedEntityTypes.Insert(2, typeof(StateMachine<,>).MakeGenericType(ammendedEntityTypes.GetRange(0,2).ToArray()));
            Types = new List<TypeInfo>() { typeof(NodeController<,,,>).MakeGenericType(ammendedEntityTypes.ToArray()).GetTypeInfo() };
        }

        public override string Name => "GenericController";
        public IEnumerable<TypeInfo> Types { get; }
    }
}
