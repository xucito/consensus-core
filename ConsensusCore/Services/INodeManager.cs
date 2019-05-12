using ConsensusCore.Enums;
using ConsensusCore.Messages;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using ConsensusCore.ViewModels;

namespace ConsensusCore.Services
{
    public interface INodeManager
    {
        NodeOptions _options { get; }
        INodeRepository _repository { get; }
        NodeInfo Information { get; }
        NodeRole CurrentRole { get; set; }
        void AppendEntry(AppendEntry entry);
    }
}