using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Services.Raft;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsensusCore.Node.Services.Tasks
{
    public class Monitor<State> where State : BaseState, new()
    {
        private IStateMachine<State> _stateMachine;
        private ILogger<Monitor<State>> _logger;
        private NodeStateService _nodeStateService;

        public Monitor(
            IStateMachine<State> stateMachine,
            ILogger<Monitor<State>> logger,
            NodeStateService nodeStateService)
        {
            _logger = logger;
            _stateMachine = stateMachine;
            _nodeStateService = nodeStateService;
        }
    }
}
