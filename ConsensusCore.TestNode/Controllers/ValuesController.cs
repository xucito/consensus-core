using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.RPCs.Shard;
using ConsensusCore.Node;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.TestNode.Models;
using Microsoft.AspNetCore.Mvc;

namespace ConsensusCore.TestNode.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        ClusterClient _clusterClient;
        TestDataRouter _router;
        NodeStateService _node;

        public ValuesController(IDataRouter router, ClusterClient clusterClient,
            NodeStateService node)
        {
            _clusterClient = clusterClient;
            _router = (TestDataRouter)router;
            _node = node;
        }
        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] int value)
        {
            if (_node.InCluster)
            {
                var newId = Guid.NewGuid();
                var result = await (_clusterClient.Send(new AddShardWriteOperation()
                {
                    Data = new TestData()
                    {
                        Data = value,
                        ShardType = "number",
                        Id = newId
                    },
                    Operation = ShardOperationOptions.Create,
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
                return Ok((await _clusterClient.Send(new RequestDataShard()
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

        [HttpGet("{id}/lock")]
        public async Task<IActionResult> GetNumberLock(Guid id)
        {
            if (_node.InCluster)
            {
                return Ok((await _clusterClient.Send(new RequestDataShard()
                {
                    ObjectId = id,
                    Type = "number",
                    CreateLock = true,
                    LockTimeoutMs = 10000
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
                return Ok(((TestDataRouter)_router)._numberStore.OrderBy(numberStore => numberStore.Key));
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
                var number = (await _clusterClient.Send(new RequestDataShard()
                {
                    ObjectId = id,
                    Type = "number"
                }));

                if (number != null && number.Data != null && number.IsSuccessful)
                {

                    var updatedObject = (TestData)number.Data;
                    updatedObject.Data = value;

                    var result = _clusterClient.Send(new AddShardWriteOperation()
                    {
                        Data = new TestData()
                        {
                            Data = value,
                            ShardType = "number",
                            Id = id
                        },
                        Operation = ShardOperationOptions.Update,
                        WaitForSafeWrite = true
                    });
                    return Ok();
                }
                return BadRequest("Object " + id + " does not exist.");
            }
            else
            {
                return StatusCode(503);
            }
        }
    }
}
