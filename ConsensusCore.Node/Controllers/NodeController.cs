using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Messages;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ConsensusCore.Node.Controllers
{
    [Route("api/node")]
    //[GenericController]
    public class NodeController<Command, State, Repository> : Controller
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where Repository : BaseRepository<Command>
    {
        private ConsensusCoreNode<Command, State, Repository> _node;
        private ILogger<NodeController<Command, State, Repository>> Logger;

        public NodeController(ConsensusCoreNode<Command, State, Repository> manager, ILogger<NodeController<Command, State, Repository>> logger)
        {
            _node = manager;
            Logger = logger;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_node.NodeInfo);
        }

        [HttpPost("request-vote")]
        public IActionResult PostRequestVote([FromBody] RequestVote vote)
        {
            return Ok(new VoteReply
            {
                Success = _node.RequestVote(vote)
            });
        }

        [HttpPost("append-entry")]
        public IActionResult PostAppendEntry([FromBody]AppendEntry<Command> entry)
        {
            try
            {
                var isSuccess = _node.AppendEntry(entry);
                return Ok();
            }
            catch(ConflictingLogEntryException e)
            {
                return BadRequest(new InvalidAppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.ConflictingLogEntryException,
                    ConflictingTerm = e.ConflictingTerm,
                    FirstTermIndex = e.FirstTermIndex
                });
            }
            catch(MissingLogEntryException e)
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

        [HttpPost("routed-command")]
        public IActionResult Post([FromBody] List<Command> command)
        {
            //Logger.LogInformation("Detect routed request from " + Request.HttpContext.Connection.RemoteIpAddress + ":" + Request.HttpContext.Connection.RemotePort);
            //return Ok(_node.ProcessCommandsAsync(command));
            return Ok();
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
    }
}
