using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.SystemCommands
{
    /*public class UpdateShard : BaseCommand
    {
        public override string CommandName => "UpdateShard";
        /// <summary>
        /// The data that you are operating on
        /// </summary>
        public Guid ShardDataId { get; set; }
    }*/

    public enum ShardOperationOptions
    {
        Create,
        /// <summary>
        /// Delete will delete the object from the index
        /// </summary>
        Delete
    }
}
