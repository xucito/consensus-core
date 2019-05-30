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
        public async Task<IActionResult> Post([FromBody] int value)
        {
            var result = await (_node.Send(new WriteData()
            {
                Data = value,
                Type = "number",
                WaitForSafeWrite = true
            }));

            if (result.IsSuccessful)
            {
                return Ok(result);
            }
            else
            {
                return StatusCode(500);
            }
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetNumber(Guid id)
        {
            return Ok(await _node.Send(new RequestDataShard()
            {
                ObjectId = id,
                Type = "number"
            }));
        }

        /* [HttpPut("{id}")]
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
         }*/
    }
}
