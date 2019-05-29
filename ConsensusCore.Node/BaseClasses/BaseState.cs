using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.SystemCommands;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.BaseClasses
{
    public abstract class BaseState
    {
        public BaseState() { }
        public Dictionary<Guid, NodeInformation> Nodes { get; set; } = new Dictionary<Guid, NodeInformation>();
        public ConcurrentDictionary<Guid, ShardMetadata> Shards { get; set; } = new ConcurrentDictionary<Guid, ShardMetadata>();

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
                case CreateDataShardInformation t1:
                    CreateDataShardInformation updateShardCommand = (CreateDataShardInformation)command;
                    var newShardmetadata = new ShardMetadata()
                    {
                        Type = updateShardCommand.Type,
                        PrimaryAllocation = updateShardCommand.PrimaryAllocation,
                        Version = updateShardCommand.Version,
                        Initalized = updateShardCommand.Initalized,
                        Allocations = updateShardCommand.Allocations,
                        DataTable = new Dictionary<Guid, DataStates>(),
                        ShardNumber = t1.ShardNumber,
                        MaxSize = t1.MaxSize,
                        Id = t1.ShardId
                    };

                    if (Shards.ContainsKey(updateShardCommand.ShardId))
                    {
                        Shards[updateShardCommand.ShardId] = newShardmetadata;
                    }
                    else
                    {
                        Shards.TryAdd(updateShardCommand.ShardId, newShardmetadata);
                    }
                    break;
                case UpdateShard t1:
                    switch (t1.Action)
                    {
                        case UpdateShardAction.Append:
                            Shards[t1.ShardId].DataTable.Add(t1.ObjectId, DataStates.Assigned);
                            break;
                        case UpdateShardAction.Delete:
                            Shards[t1.ShardId].DataTable.Remove(t1.ObjectId);
                            break;
                        case UpdateShardAction.Update:
                            throw new Exception("TO DO NOT IMPLEMENTED");
                        case UpdateShardAction.Initialize:
                            Shards[t1.ShardId].DataTable[t1.ObjectId] = DataStates.Initialized;
                            break;
                    }
                    //Only the primary can update version so it's assumed the primary has written this

                    if (Shards.ContainsKey(t1.ShardId))
                    {
                        if (Shards[t1.ShardId].Allocations.ContainsKey(Shards[t1.ShardId].PrimaryAllocation))
                        {
                            Shards[t1.ShardId].Allocations[Shards[t1.ShardId].PrimaryAllocation] = Shards[t1.ShardId].Version + 1;
                        }
                        else
                        {
                            Shards[t1.ShardId].Allocations.Add(Shards[t1.ShardId].PrimaryAllocation, Shards[t1.ShardId].Version + 1);
                        }

                        //There is a split second where the update may not happen
                        Shards[t1.ShardId].Version++;
                    }
                    break;
                /*
                if (updateShardAllocation.Version == -1)
                {
                    if (Shards[updateShardAllocation.ShardId].Allocations.ContainsKey(updateShardAllocation.NodeId))
                    {
                        Shards[updateShardAllocation.ShardId].Allocations.Remove(updateShardAllocation.NodeId);
                    }
                }
                //Update the version or add the allocation
                else
                {
                    if (Shards.ContainsKey(updateShardAllocation.ShardId))
                    {
                        if (Shards[updateShardAllocation.ShardId].Allocations.ContainsKey(updateShardAllocation.NodeId))
                        {
                            Shards[updateShardAllocation.ShardId].Allocations[updateShardAllocation.NodeId] = updateShardAllocation.Version;
                        }
                        else
                        {
                            Shards[updateShardAllocation.ShardId].Allocations.Add(updateShardAllocation.NodeId, updateShardAllocation.Version);
                        }

                        //There is a split second where the update may not happen

                        if (updateShardAllocation.Version > Shards[updateShardAllocation.ShardId].Version)
                        {
                            Shards[updateShardAllocation.ShardId].Version = updateShardAllocation.Version;
                        }
                    }
                }
                break;*/
                default:
                    ApplyCommandToState(command);
                    break;
            }
        }

        public abstract void ApplyCommandToState(BaseCommand command);
    }
}
