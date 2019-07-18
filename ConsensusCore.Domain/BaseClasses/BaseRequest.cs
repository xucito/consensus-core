using ConsensusCore.Domain.Serializers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    [JsonConverter(typeof(BaseRequestConverter))]
    public interface IClusterRequest<out TResponse> where TResponse: BaseResponse
    {
        string RequestName { get; }
    }

    [JsonConverter(typeof(BaseRequestConverter))]
    public abstract class BaseRequest<T> : BaseRequest, IClusterRequest<T> where T:BaseResponse
    {
        public override abstract string RequestName { get; }
    }

    [JsonConverter(typeof(BaseRequestConverter))]
    public abstract class BaseRequest
    {
        public abstract string RequestName { get; }
    }
}
