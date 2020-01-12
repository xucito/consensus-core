using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Services;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IBaseRepository<State> where State : BaseState, new()
    {
        Task<bool> SaveNodeDataAsync(NodeStorage<State> storage);
        Task<NodeStorage<State>> LoadNodeDataAsync();
    }
}