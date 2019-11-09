using ConsensusCore.BaseClasses;
using ConsensusCore.Clients;
using ConsensusCore.Enums;
using ConsensusCore.Interfaces;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Services
{
    public class ConsensusCoreNode<Command, State, StateMachine, Repository> :
        INodeManager<Command, State, StateMachine, Repository>
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where StateMachine : StateMachine<Command, State>
        where Repository : INodeRepository<Command>
    {

        public NodeOptions _nodeOptions { get; }
        public ClusterOptions _clusterOptions { get; }
        public INodeRepository<Command> _repository { get; }
        
        public ConsensusCoreNode(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<NodeOptions> options,
            INodeRepository<Command> repository,
            ILogger<NodeManager<Command,
            State,
            StateMachine,
            Repository>> logger)
        {
            _nodeOptions = options.Value;
            _clusterOptions = clusterOptions.Value;
            _repository = repository;
            Information = repository.LoadConfiguration();
            ConsensusCoreNodeClient.SetTimeout(TimeSpan.FromMilliseconds(_clusterOptions.LatencyToleranceMs));
            CurrentRole = NodeRole.Follower;
            Logger = logger;
        }


    }
}
