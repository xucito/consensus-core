using ConsensusCore.BaseClasses;
using ConsensusCore.Enums;
using ConsensusCore.Interfaces;
using ConsensusCore.Messages;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using ConsensusCore.ViewModels;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsensusCore.Services
{
    public interface INodeManager<Command, State, StateMachine, Repository>
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where StateMachine : StateMachine<Command, State>
        where Repository : INodeRepository<Command>
    {
        NodeOptions _options { get; }
        INodeRepository<Command> _repository { get; }
        NodeInfo<Command> Information { get; }
        NodeRole CurrentRole { get; set; }
        Task<bool> ProcessLogEntriesAsync(List<LogEntry<Command>> command, int timeoutMs = 10000);
        bool AppendEntry(AppendEntry<Command> entry);
        bool RequestVote(RequestVote vote);
        State GetState();
        Task<bool> ProcessCommandsAsync(List<Command> commands, int timeoutMs = 10000);
    }
}