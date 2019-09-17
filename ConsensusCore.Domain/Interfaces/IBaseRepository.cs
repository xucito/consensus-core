using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Services;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IBaseRepository
    {
        void SaveNodeData<Z>(NodeStorage<Z> storage) where Z : BaseState, new();
        NodeStorage<Z> LoadNodeData<Z>() where Z : BaseState, new();
    }
}