using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.SystemCommands.Tasks;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.BaseClasses
{
    public abstract class BaseState
    {
        public BaseState() { }
        public ConcurrentDictionary<Guid, NodeInformation> Nodes { get; set; } = new ConcurrentDictionary<Guid, NodeInformation>();
        public ConcurrentDictionary<string, Index> Indexes { get; set; } = new ConcurrentDictionary<string, Index>();
        public ConcurrentDictionary<Guid, BaseTask> ClusterTasks { get; set; } = new ConcurrentDictionary<Guid, BaseTask>();
        //object id and Shard id
        public ConcurrentDictionary<Guid, ObjectLock> ObjectLocks = new ConcurrentDictionary<Guid, ObjectLock>();

        public BaseTask GetRunningTask(string uniqueTaskId)
        {
            var searchedTasks = ClusterTasks.Where(ct => ct.Value.UniqueRunningId == uniqueTaskId && ct.Value.CompletedOn == null);

            if (searchedTasks.Count() == 0)
                return null;
            else
            {
                return searchedTasks.FirstOrDefault().Value;
            }
        }

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
                            TransportAddress = t1.TransportAddress,
                            Id = t1.Id,
                            IsContactable = t1.IsContactable
                        };
                    }
                    else
                    {
                        Nodes.TryAdd(t1.Id, new NodeInformation()
                        {
                            Name = t1.Name,
                            TransportAddress = t1.TransportAddress,
                            Id = t1.Id,
                            IsContactable = t1.IsContactable
                        });
                    }
                    break;
                case DeleteNodeInformation t1:
                    if (Nodes.ContainsKey(t1.Id))
                    {
                        Nodes.TryRemove(t1.Id, out _);
                    }
                    break;
                case CreateIndex t1:
                    Indexes.TryAdd(t1.Type, new Index()
                    {
                        Shards = t1.Shards,
                        Type = t1.Type
                    });
                    break;
                case UpdateClusterTasks t1:
                    if (t1.TasksToAdd != null)
                    {
                        foreach (var task in t1.TasksToAdd)
                        {
                            if (!ClusterTasks.TryAdd(task.Id, task))
                            {
                                //Can't add a task twice
                                if (ClusterTasks.ContainsKey(task.Id))
                                {
                                    Console.WriteLine("Critical error while trying to add cluster task " + task.Id + " the id already exists as the object " + JsonConvert.SerializeObject(ClusterTasks[task.Id], Formatting.Indented));
                                }
                                else
                                {
                                    throw new Exception("Critical error while trying to add cluster task " + task.Id);
                                }
                            }
                        }
                    }
                    if (t1.TasksToRemove != null)
                    {
                        foreach (var task in t1.TasksToRemove)
                        {
                            if (!ClusterTasks.TryRemove(task, out _))
                            {
                                throw new Exception("Critical error while trying to remove cluster task " + task);
                            }
                        }
                    }
                    if (t1.TasksToUpdate != null)
                    {
                        foreach (var task in t1.TasksToUpdate)
                        {
                            ClusterTasks[task.TaskId].CompletedOn = task.CompletedOn;
                            ClusterTasks[task.TaskId].Status = task.Status;
                        }
                    }
                    break;
                case UpdateShardMetadata t1:
                    if (!t1.IgnoreAllocations)
                    {
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().PrimaryAllocation = t1.PrimaryAllocation;
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations = t1.InsyncAllocations;
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations = t1.StaleAllocations;
                    }
                    break;
                case UpdateShardMetadataAllocations t1:
                    if (t1.InsyncAllocationsToAdd != null)
                    {
                        foreach (var allocation in t1.InsyncAllocationsToAdd)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations.Add(allocation);
                        }
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations = Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations.Where(sa => !t1.InsyncAllocationsToAdd.Contains(sa)).ToHashSet();
                    }
                    if (t1.InsyncAllocationsToRemove != null)
                    {
                        foreach (var allocation in t1.InsyncAllocationsToRemove)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations.Remove(allocation);
                        }
                    }
                    if (t1.StaleAllocationsToAdd != null)
                    {
                        foreach (var allocation in t1.StaleAllocationsToAdd)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations.Add(allocation);
                        }
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations = Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations.Where(sa => !t1.StaleAllocationsToAdd.Contains(sa)).ToHashSet();
                    }
                    if (t1.StaleAllocationsToRemove != null)
                    {
                        foreach (var allocation in t1.StaleAllocationsToRemove)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations.Remove(allocation);
                        }
                    }
                    if (t1.LatestPos != null)
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().LatestOperationPos = t1.LatestPos.Value;
                    break;
                case SetObjectLock t1:
                    var result = ObjectLocks.TryAdd(t1.ObjectId, new ObjectLock()
                    {
                        LockTimeoutMs = t1.TimeoutMs,
                        ObjectId = t1.ObjectId,
                        Type = t1.Type
                    });
                    if (!result)
                    {
                        throw new ConflictingObjectLockException("Object " + t1.ObjectId + " is already locked.");
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
