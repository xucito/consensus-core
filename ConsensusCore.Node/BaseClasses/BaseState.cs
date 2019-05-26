using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.SystemCommands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.BaseClasses
{
    public abstract class BaseState
    {
        public BaseState() { }
        public Dictionary<Guid, NodeInformation> Nodes { get; set; } = new Dictionary<Guid, NodeInformation>();
        public Dictionary<Guid, ShardMetadata> Shards { get; set; } = new Dictionary<Guid, ShardMetadata>();

        public void ApplyCommand(BaseCommand command)
        {
            switch (command)
            {
                case UpsertNodeInformation t1:
                    UpsertNodeInformation convertedCommand = (UpsertNodeInformation)(object)command;
                    if (Nodes.ContainsKey(convertedCommand.Id))
                    {
                        Nodes[convertedCommand.Id] = new NodeInformation()
                        {
                            Name = convertedCommand.Name,
                            TransportAddress = convertedCommand.TransportAddress
                        };
                    }
                    else
                    {
                        Nodes.Add(convertedCommand.Id, new NodeInformation()
                        {
                            Name = convertedCommand.Name,
                            TransportAddress = convertedCommand.TransportAddress
                        });
                    }
                    break;
                case DeleteNodeInformation t1:
                    DeleteNodeInformation deleteCommand = (DeleteNodeInformation)(object)command;
                    if (Nodes.ContainsKey(deleteCommand.Id))
                    {
                        Nodes.Remove(deleteCommand.Id);
                    }
                    break;
                case UpsertDataShardInformation t1:
                    UpsertDataShardInformation updateShardCommand = (UpsertDataShardInformation)command;
                    var newShardmetadata = new ShardMetadata()
                    {
                        Type = updateShardCommand.Type,
                        PrimaryAllocation = updateShardCommand.PrimaryAllocation,
                        Version = updateShardCommand.Version,
                        Initalized = updateShardCommand.Initalized,
                        Allocations = updateShardCommand.Allocations
                    };

                    if (Shards.ContainsKey(updateShardCommand.ShardId))
                    {
                        Shards[updateShardCommand.ShardId] = newShardmetadata;
                    }
                    else
                    {
                        Shards.Add(updateShardCommand.ShardId, newShardmetadata);
                    }
                    break;
                default:
                    ApplyCommandToState(command);
                    break;
            }
        }

        public abstract void ApplyCommandToState(BaseCommand command);
    }
}
