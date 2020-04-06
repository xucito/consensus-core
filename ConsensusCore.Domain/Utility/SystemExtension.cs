using Force.DeepCloner;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Utility
{
    public static class SystemExtension
    {
        public static T Clone<T>(this T source)
        {
            return source.DeepClone();
        }
    }
}
