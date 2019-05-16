using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Interfaces;
using ConsensusCore.Repositories;
using ConsensusCore.Samples.Calculator;
using ConsensusCore.Services;
using Microsoft.AspNetCore.Mvc;

namespace ConsensusCore.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CalculatorController : ControllerBase
    {
        INodeManager<Calculate, CalculatorState, StateMachine<Calculate, CalculatorState>, NodeInMemoryRepository<Calculate>> _nodeManager { get; set; }

        public CalculatorController(INodeManager<Calculate, CalculatorState, StateMachine<Calculate, CalculatorState>, NodeInMemoryRepository<Calculate>> nodeManager)
        {
            _nodeManager = nodeManager;
        }

        [HttpPost]
        public async Task<IActionResult> AddCalculation([FromBody] Calculate calculate)
        {
            await _nodeManager.ProcessCommandsAsync(new List<Calculate>() { calculate });
            return Ok();
        }

        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] { "value1", "value2" };
        }
    }
}
