using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Models;
using ConsensusCore.Node.SystemCommands;
using ConsensusCore.Node.ValueObjects;
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
        public List<BaseTask> ClusterTasks { get; set; } = new List<BaseTask>();

        public void ApplyCommand(BaseCommand command)
        {
            switch (command)
            {
                case UpsertNodeInformation t1:
                    if (Nodes.ContainsKey(t1.Id))
                    {
                        Nodes[t1.Id] = new NodeInformation()
                        {
                            Name = t1.Name,
                            TransportAddress = t1.TransportAddress
                        };
                    }
                    else
                    {
                        Nodes.Add(t1.Id, new NodeInformation()
                        {
                            Name = t1.Name,
                            TransportAddress = t1.TransportAddress
                        });
                    }
                    break;
                case DeleteNodeInformation t1:
                    if (Nodes.ContainsKey(t1.Id))
                    {
                        Nodes.Remove(t1.Id);
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
                        DataTable = new ConcurrentDictionary<Guid, ShardData>(),
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

                        switch (update.Value.Action)
                        {
                            case UpdateShardAction.Append:
                                // If this returns false it is because the object already exists                                
                                lock (uninitalizedObjectsLock)
                                {
                                    var wasItAdded = Shards[update.Value.ShardId].DataTable.TryAdd(update.Value.DataId,
                                        new ShardData()
                                        {
                                            State = DataStates.Assigned,
                                            Version = Shards[update.Value.ShardId].Version
                                        });
                                    if (wasItAdded)
                                    {
                                        UninitializedObjects.Add(update.Value.DataId, update.Value.ShardId);
                                    }
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
                                    //Change the state of the data and also the version that this document was updated
                                    Shards[update.Value.ShardId].DataTable[update.Value.DataId].State = DataStates.Initialized;
                                    Shards[update.Value.ShardId].DataTable[update.Value.DataId].Version = Shards[update.Value.ShardId].Version;
                                    UninitializedObjects.Remove(update.Value.DataId);
                                }
                                break;
                        }



                    }
                    break;
                case UpsertClusterTasks t1:
                    foreach (var task in t1.ClusterTasks)
                    {
                        switch (task.Status)
                        {
                            case Enums.ClusterTaskStatuses.Created:
                                ClusterTasks.Add(task);
                                break;
                        }
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
