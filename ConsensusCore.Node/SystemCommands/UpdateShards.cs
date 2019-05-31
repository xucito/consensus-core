using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.ValueObjects;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    public class UpdateShards : BaseCommand
    {
        public Dictionary<Guid, ShardDataUpdate> Updates { get; set; }
        /// <summary>
        /// Used for 
        /// </summary>
        public override string CommandName => "UpdateShard";
    }

    public enum UpdateShardAction
    {
        /// <summary>
        /// Append will add the object to the list of stored objects as uninitialized
        /// </summary>
        Append,
        /// <summary>
        /// Update will delete the object from the point in the dictionary and readd it below
        /// </summary>
        Update,
        /// <summary>
        /// Delete will delete the object from the index
        /// </summary>
        Delete,
        /// <summary>
        /// Initalize will mark the shard as initialized
        /// </summary>
        Initialize
    }
}
