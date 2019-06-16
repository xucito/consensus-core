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
        TestDataRouter _router;

        public ValuesController(IConsensusCoreNode<TestState, NodeInMemoryRepository> node, IDataRouter router)
        {
            _node = node;
            _router = (TestDataRouter)router;
        }
        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] int value)
        {
            if (_node.InCluster)
            {
                var newId = Guid.NewGuid();
                var result = await (_node.Send(new WriteData()
                {
                    Data = new TestData()
                    {
                        Data = value,
                        Type = "number",
                        Id = newId
                    },
                    Operation = Node.Enums.ShardOperationOptions.Create,
                    WaitForSafeWrite = true
                }));

                if (result.IsSuccessful)
                {
                    return Ok(newId);
                }
                else
                {
                    return StatusCode(500);
                }
            }
            else
            {
                return StatusCode(503);

            }
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetNumber(Guid id)
        {
            if (_node.InCluster)
            {
                return Ok((await _node.Send(new RequestDataShard()
                {
                    ObjectId = id,
                    Type = "number"
                })));
            }
            else
            {
                return StatusCode(503);

            }
            //return Ok(_router._numberStore.Where(n => n.Id == id));
        }

        [HttpGet("Test")]
        public async Task<IActionResult> Number()
        {
            if (_node.InCluster)
            {
                return Ok(((TestDataRouter)_router)._numberStore);
            }
            else
            {

                return StatusCode(503);
            }
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateValue(Guid id, [FromBody] int value)
        {
            if (_node.InCluster)
            {
                var number = (await _node.Send(new RequestDataShard()
                {
                    ObjectId = id,
                    Type = "number"
                }));

                var updatedObject = (TestData)number.Data;
                updatedObject.Data = value;

                var result = _node.Send(new WriteData()
                {
                    Data = new TestData()
                    {
                        Data = value,
                        Type = "number",
                        Id = id
                    },
                    Operation = Node.Enums.ShardOperationOptions.Update,
                    WaitForSafeWrite = true
                });
                return Ok();
            }
            else
            {
                return StatusCode(503);
            }
        }
    }
}
