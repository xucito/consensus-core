using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Node;
using ConsensusCore.Node.Repositories;
using ConsensusCore.TestNode.Models;
using Microsoft.AspNetCore.Mvc;

namespace ConsensusCore.TestNode.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        IConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>> _node;

        public ValuesController(IConsensusCoreNode<TestCommand, TestState, NodeInMemoryRepository<TestCommand>> node)
        {
            _node = node;
        }
        // POST api/values
        [HttpPost]
        public void Post([FromBody] int value)
        {
            _node.AddCommand(new List<TestCommand>()
            {
                new TestCommand()
                {
                    ValueAdd = value
                }
            }, true);
        }
    }
}
