using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.BaseClasses;
using ConsensusCore.Exceptions;
using ConsensusCore.Interfaces;
using ConsensusCore.Messages;
using ConsensusCore.Repositories;
using ConsensusCore.Services;
using ConsensusCore.Utility;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ConsensusCore.Controllers
{
    [Route("api/node")]
    //[GenericController]
    public class NodeController<Command, State, StateMachine, Repository> : Controller
        where Command : BaseCommand
        where State : BaseState<Command>, new()
        where StateMachine : StateMachine<Command, State>
        where Repository : INodeRepository<Command>
    {
        private INodeManager<Command, State, StateMachine, Repository> _manager;
        private ILogger<NodeController<Command, State, StateMachine, Repository>> Logger;

        public NodeController(INodeManager<Command, State, StateMachine, Repository> manager, ILogger<NodeController<Command, State, StateMachine, Repository>> logger)
        {
            _manager = manager;
            Logger = logger;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_manager.Information);
        }

        [HttpPost("request-vote")]
        public IActionResult PostRequestVote([FromBody] RequestVote vote)
        {
            return Ok(new VoteReply
            {
                Success = _manager.RequestVote(vote),
                NodeId = _manager.Information.Id
            });
        }

        [HttpPost("append-entry")]
        public IActionResult PostAppendEntry([FromBody]AppendEntry<Command> entry)
        {
            try
            {
                var isSuccess = _manager.AppendEntry(entry);
                return Ok();
            }
            catch(ConflictingLogEntryException e)
            {
                return BadRequest(new InvalidAppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.ConflictingLogEntryException,
                    ConflictingTerm = e.ConflictingTerm,
                    LastLogEntryIndex = _manager.Information.Logs.Count(),
                    FirstTermIndex = e.FirstTermIndex
                });
            }
            catch(MissingLogEntryException e)
            {
                return BadRequest(new InvalidAppendEntryResponse()
                {
                    ConflictName = AppendEntriesExceptionNames.MissingLogEntryException,
                    ConflictingTerm = null,
                    LastLogEntryIndex = _manager.Information.Logs.Count(),
                    FirstTermIndex = null
                });
            }
        }

        [HttpPost("routed-command")]
        public IActionResult Post([FromBody] List<Command> command)
        {
            Logger.LogInformation("Detect routed request from " + Request.HttpContext.Connection.RemoteIpAddress + ":" + Request.HttpContext.Connection.RemotePort);
            return Ok(_manager.ProcessCommandsAsync(command));
        }

        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        [HttpGet("state")]
        public IActionResult GetState()
        {
            return Ok(_manager.GetState());
        }
    }
}
