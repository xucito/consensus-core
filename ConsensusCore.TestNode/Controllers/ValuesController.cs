using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Node;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.RPCs;
using ConsensusCore.Node.Services;
using ConsensusCore.TestNode.Models;
using Microsoft.AspNetCore.Mvc;

namespace ConsensusCore.TestNode.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        IConsensusCoreNode<TestState, NodeInMemoryRepository> _node;

        public ValuesController(IConsensusCoreNode<TestState, NodeInMemoryRepository> node)
        {
            _node = node;
        }
        // POST api/values
        [HttpPost]
        public IActionResult Post([FromBody] int value)
        {
            var result = (_node.Send(new WriteDataShard()
            {
                Data = value,
                Type = "number",
                ShardId = null,
                WaitForSafeWrite = true
            }));

            if(result.IsSuccessful)
            {
                return Ok(result);
            }
            else
            {
                return StatusCode(500);
            }
        }

        [HttpGet("{id}")]
        public IActionResult GetNumber(Guid id)
        {
            return Ok(_node.Send(new RequestDataShard()
            {
                ShardId = id,
                Type = "number"
            }));
        }

        [HttpPut("{id}")]
        public void UpdateValue(Guid id, [FromBody] int value)
        {
            var result = _node.Send(new WriteDataShard()
            {
                Data = value,
                ShardId = id,
                Type = "number",
                WaitForSafeWrite = true
                //Need to pull the previous version
            });
            // _node.UpdateShardCommand(id, "number", value);
        }
    }
}
