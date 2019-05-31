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
        public Dictionary<Guid, Guid> UninitializedObjects { get; set; } = new Dictionary<Guid, Guid>();
        public object uninitalizedObjectsLock = new object();

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
                        DataTable = new ConcurrentDictionary<Guid, DataStates>(),
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
                case UpdateShards t1:
                    //To keep track of all shards that you update
                    List<Guid> UpdatedShards = new List<Guid>();
                    foreach (var update in t1.Updates)
                    {
                        switch (update.Value.Action)
                        {
                            case UpdateShardAction.Append:
                                // If this returns false it is because the object already exists                                
                                lock (uninitalizedObjectsLock)
                                {
                                    var wasItAdded = Shards[update.Value.ShardId].DataTable.TryAdd(update.Value.DataId, DataStates.Assigned);
                                    UninitializedObjects.Add(update.Value.DataId, update.Value.ShardId);
                                }
                                break;
                            case UpdateShardAction.Delete:
                                //If this return false it is because the object is already gone
                                var wasItRemoved = Shards[update.Value.ShardId].DataTable.TryRemove(update.Value.DataId, out _);
                                break;
                            case UpdateShardAction.Update:
                                throw new Exception("TO DO NOT IMPLEMENTED");
                            case UpdateShardAction.Initialize:
                                lock (uninitalizedObjectsLock)
                                {
                                    Shards[update.Value.ShardId].DataTable[update.Value.DataId] = DataStates.Initialized;
                                    UninitializedObjects.Remove(update.Value.DataId);
                                }
                                break;
                        }


                        if (Shards.ContainsKey(update.Value.ShardId) && !UpdatedShards.Contains(update.Value.ShardId))
                        {
                            if (Shards[update.Value.ShardId].Allocations.ContainsKey(Shards[update.Value.ShardId].PrimaryAllocation))
                            {
                                Shards[update.Value.ShardId].Allocations[Shards[update.Value.ShardId].PrimaryAllocation] = Shards[update.Value.ShardId].Version + 1;
                            }
                            else
                            {
                                Shards[update.Value.ShardId].Allocations.Add(Shards[update.Value.ShardId].PrimaryAllocation, Shards[update.Value.ShardId].Version + 1);
                            }

                            //There is a split second where the update may not happen
                            Shards[update.Value.ShardId].Version++;
                            UpdatedShards.Add(update.Value.ShardId);
                        }
                    }
                    break;
                //Only the primary can update version so it's assumed the primary has written this

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
