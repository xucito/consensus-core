using ConsensusCore.Node.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    [JsonConverter(typeof(BaseRequestConverter))]
    public interface IClusterRequest<out TResponse>
    {
        string RequestName { get; }
    }

    [JsonConverter(typeof(BaseRequestConverter))]
    public abstract class BaseRequest<T> : BaseRequest, IClusterRequest<T>
    {
        public override abstract string RequestName { get; }
    }

    [JsonConverter(typeof(BaseRequestConverter))]
    public abstract class BaseRequest
    {
        public abstract string RequestName { get; }
    }
}
