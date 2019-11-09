using Microsoft.AspNetCore.Mvc.ApplicationModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Utility
{
    public class GenericControllerAttribute : Attribute, IControllerModelConvention
    {
        public void Apply(ControllerModel controller)
        {
            Type entityType = controller.ControllerType.GetGenericArguments()[0];
            controller.ControllerName = entityType.Name;
        }
    }
}
