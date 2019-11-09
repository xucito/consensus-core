using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Services;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IBaseRepository<State> where State : BaseState, new()
    {
        void SaveNodeData(NodeStorage<State> storage);
        NodeStorage<State> LoadNodeData();
    }
}