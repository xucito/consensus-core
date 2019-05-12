using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Messages;
using ConsensusCore.Services;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ConsensusCore.Controllers
{
    [Route("api/[controller]")]
    public class NodeController : Controller
    {
        private INodeManager _manager;

        public NodeController(INodeManager manager)
        {
            _manager = manager;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_manager.Information);
        }

        [HttpPost("request-vote")]
        public IActionResult PostRequestVote([FromBody] RequestVote vote)
        {
            if (_manager.Information.CurrentTerm < vote.Term)
            {
                if (_manager.Information.Logs.Count() <= vote.LastLogIndex && (_manager.Information.VotedFor == null || _manager.Information.VotedFor == vote.CandidateId))
                {
                    _manager.Information.VotedFor = vote.CandidateId;
                    return Ok(new VoteReply
                    {
                        Success = true,
                        NodeId = _manager.Information.Id
                    });
                }
            }

            return Ok(new VoteReply
            {
                Success = false,
                NodeId = _manager.Information.Id
            });
        }

        [HttpPost("append-entry")]
        public IActionResult PostAppendEntry([FromBody]AppendEntry entry)
        {
            _manager.AppendEntry(entry);
            return Ok(_manager.Information);
        }

        [HttpPost]
        public void Post([FromBody]string value)
        {
        }

        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }
    }
}
