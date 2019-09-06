using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public class ShardManager<State, Repository>
        where State : BaseState, new()
        where Repository : IBaseRepository
    {
        public NodeStorage _nodeStorage { get; set; }
        ILogger<ShardManager<State, Repository>> Logger { get; set; }

        public ShardManager(IStateMachine<State> state,
            Repository repository,
            ILogger<ShardManager<State, Repository>> logger)
        {
            var storage = repository.LoadNodeData();
            if (storage != null)
            {
                Logger.LogInformation("Loaded Consensus Local Node storage from store");
                _nodeStorage = storage;
                _nodeStorage.SetRepository(repository);
            }
            else
            {
                Logger.LogError("Could not initialize shard manager as node storage is empty.");
                throw new Exception("Could not initialize shard manager as node storage is empty.");
            }
        }


    }
}
