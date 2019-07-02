using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Services;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IBaseRepository
    {
        void SaveNodeData(NodeStorage storage);
        NodeStorage LoadNodeData();
    }
}