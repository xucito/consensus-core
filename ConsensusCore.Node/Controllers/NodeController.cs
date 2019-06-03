using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ConsensusCore.Node.Controllers
{
    [Route("api/node")]
    //[GenericController]
    public class NodeController<State, Repository> : Controller
        where State : BaseState, new()
        where Repository : BaseRepository
    {
        private IConsensusCoreNode<State, Repository> _node;
        private ILogger<NodeController<State, Repository>> Logger;

        public NodeController(IConsensusCoreNode<State, Repository> manager, ILogger<NodeController<State, Repository>> logger)
        {
            _node = manager;
            Logger = logger;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_node.NodeInfo);
        }

        [HttpGet("localShards")]
        public IActionResult GetLocalShards()
        {
            return Ok(_node.LocalShards);
        }

        [HttpPost("RPC")]
        public async Task<IActionResult> PostRPC([FromBody]IClusterRequest<object> request)
        {
            return Ok(await _node.Send(request));
        }
        /*
        [HttpPost("request-vote")]
        public IActionResult PostRequestVote([FromBody] RequestVote vote)
        {
            return Ok(new VoteReply
            {
                Success = _node.RequestVote(vote)
            });
        }

        [HttpPost("append-entry")]
        public IActionResult PostAppendEntry([FromBody]AppendEntry entry)
        {
            try
            {
                var isSuccess = _node.AppendEntry(entry);
                return Ok();
            }
            catch (ConflictingLogEntryException e)
            {
                return BadRequest(new InvalidAppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.ConflictingLogEntryException,
                    ConflictingTerm = e.ConflictingTerm,
                    FirstTermIndex = e.FirstTermIndex
                });
            }
            catch (MissingLogEntryException e)
            {
                return BadRequest(new InvalidAppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.MissingLogEntryException,
                    ConflictingTerm = null,
                    LastLogEntryIndex = e.LastLogEntryIndex,
                    FirstTermIndex = null
                });
            }
        }
        


        /// <summary>
        /// Allocate a shard to this noce
        /// </summary>
        /// <param name="data"></param>
        /// <param name="type"></param>
        /// <param name="shardId"></param>
        /// <returns></returns>
        [HttpPost("assign-shard-command/{type}/{id}")]
        public IActionResult Post([FromBody] AssignDataShard shard)
        {
            _node.AssignDataShard(shard);
            return Ok();
        }

        [HttpPost("routed-request")]
        public IActionResult HandleRouteDataShardRequest([FromBody] BaseRequest request)
        {
            switch (request)
            {
                case ProcessCommandsRequest t1:
                    return Ok(_node.Send((ProcessCommandsRequest) request));
                case CreateDataShardRequest t1:
                    return Ok(_node.CreateNewShardRequestHandler((CreateDataShardRequest)request));
            }
            return BadRequest("Request did not match a valid routed-request");
        }

        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        [HttpGet("state")]
        public IActionResult GetState()
        {
            return Ok(_node.GetState());
        }

        [HttpGet("shards/{type}/{id}")]
        public IActionResult GetData(string type, Guid id)
        {
            return Ok(_node.GetData(id, type));
        }
    }
    */

        [HttpGet("state")]
        public IActionResult GetState()
        {
            return Ok(_node.GetState());
        }
    }
}