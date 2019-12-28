using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public class BaseService<State> where State: BaseState, new()
    {
        private ILogger _logger;
        protected ILogger Logger => _logger;
        public ClusterOptions ClusterOptions { get; }
        public NodeOptions NodeOptions { get; }
        public IStateMachine<State> StateMachine { get; }
        public NodeStateService NodeStateService { get; }

        public BaseService(ILogger logger, ClusterOptions clusterOptions, NodeOptions nodeOptions, IStateMachine<State> stateMachine, NodeStateService nodeState)
        {
            _logger = logger;
            ClusterOptions = clusterOptions;
            NodeOptions = nodeOptions;
            StateMachine = stateMachine;
            NodeStateService = nodeState;
        }
    }
}
