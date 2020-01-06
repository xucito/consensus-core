using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.RPCs.Shard;

namespace ConsensusCore.Node.Services.Data
{
    public interface IDataService
    {
        Task<AddShardWriteOperationResponse> AddShardWriteOperationHandler(AddShardWriteOperation request);
        Task<AllocateShardResponse> AllocateShardHandler(AllocateShard shard);
        Task<TResponse> Handle<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse, new();
        Task<ReplicateShardWriteOperationResponse> ReplicateShardWriteOperationHandler(ReplicateShardWriteOperation request);
        Task<RequestCreateIndexResponse> RequestCreateIndexHandler(RequestCreateIndex request);
        Task<RequestShardWriteOperationsResponse> RequestShardWriteOperationsHandler(RequestShardWriteOperations request);
    }
}