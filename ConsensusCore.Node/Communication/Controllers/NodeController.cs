using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Node.Communication.Clients;
using ConsensusCore.Node.Communication.Controllers;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.ViewModels;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ConsensusCore.Node.Controllers
{
    [Route("api/node")]
    //[GenericController]
    public class NodeController<State> : Controller
        where State : BaseState, new()
    {
        private ILogger<NodeController<State>> Logger;
        private IClusterRequestHandler _handler;
        private NodeStateService _nodeStateService;
        private IStateMachine<State> _stateMachine;
        private INodeStorage<State> _nodeStorage;
        private IClusterConnectionPool _clusterConnectionPool;
        private IShardRepository _shardRepository;

        public NodeController(IClusterRequestHandler handler,
            ILogger<NodeController<State>> logger,
            NodeStateService nodeStateService,
             IStateMachine<State> stateMachine,
             INodeStorage<State> nodeStorage,
             IClusterConnectionPool clusterConnectionPool,
             IShardRepository shardRepository)
        {
            _handler = handler;
            Logger = logger;
            _nodeStateService = nodeStateService;
            _stateMachine = stateMachine;
            _nodeStorage = nodeStorage;
            _clusterConnectionPool = clusterConnectionPool;
            _shardRepository = shardRepository;
        }

        [HttpPost("RPC")]
        public async Task<IActionResult> PostRPC([FromBody]IClusterRequest<BaseResponse> request)
        {
            if (request == null)
            {
                Logger.LogError("FOR SOME REASON THE REQUEST IS NULL");
            }
            return Ok(await _handler.Handle(request));
        }

        [HttpGet("state")]
        public IActionResult GetState()
        {
            return Ok(_stateMachine.CurrentState);
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_nodeStateService);
        }

        [HttpGet("logs")]
        public IActionResult GetLogs()
        {
            return Ok(_nodeStorage.Logs);
        }

        [HttpGet("clients")]
        public IActionResult GetClients()
        {
            return Ok(_clusterConnectionPool.GetAllNodeClients());
        }

        [HttpGet("transactions/{shardId}")]
        public async Task<IActionResult> GetTransactions(Guid shardId)
        {
            return Ok((await _shardRepository.GetAllShardWriteOperationsAsync(shardId)).OrderBy(swo => swo.Pos));
        }



        [HttpGet("snapshot")]
        public async Task<IActionResult> GetSnapshot()
        {
            return Ok(new
            {
                lastIncludedTerm = _nodeStorage.LastSnapshotIncludedTerm,
                lastIncludedIndex = _nodeStorage.LastSnapshotIncludedIndex,
                snapshot = _nodeStorage.LastSnapshot
            });
        }
    }
}