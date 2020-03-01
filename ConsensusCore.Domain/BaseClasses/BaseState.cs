using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.StateObjects;
using ConsensusCore.Domain.SystemCommands;
using ConsensusCore.Domain.SystemCommands.ShardMetadata;
using ConsensusCore.Domain.SystemCommands.Tasks;
using ConsensusCore.Domain.Utility;
using Microsoft.Extensions.Logging;
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
        private ILogger _logger;
        public ConcurrentDictionary<Guid, NodeInformation> Nodes { get; set; } = new ConcurrentDictionary<Guid, NodeInformation>();
        public ConcurrentDictionary<string, Index> Indexes { get; set; } = new ConcurrentDictionary<string, Index>();
        public ConcurrentDictionary<Guid, BaseTask> ClusterTasks { get; set; } = new ConcurrentDictionary<Guid, BaseTask>();
        public ConcurrentDictionary<string, Lock> Locks = new ConcurrentDictionary<string, Lock>();

        public BaseState()
        {
        }

        public BaseState(ILogger logger)
        {
            _logger = logger;
        }

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

        public IEnumerable<BaseTask> GetNodeClusterTasks(ClusterTaskStatuses[] statuses, Guid nodeId)
        {
            return SystemExtension.Clone(ClusterTasks.Where(ct => statuses.Contains(ct.Value.Status) && ct.Value.NodeId == nodeId).Select(b => b.Value));
        }

        public void ApplyCommand(BaseCommand command)
        {
            try
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
                            //Only add if it is marking as contactable
                            if (t1.IsContactable)
                            {
                                var nodes = Nodes.Where(n => n.Value.TransportAddress == t1.TransportAddress);
                                if (nodes.Count() > 0)
                                {
                                    foreach (var node in nodes)
                                    {
                                        Nodes.Remove(node.Key, out _);
                                    }
                                }
                                Nodes.TryAdd(t1.Id, new NodeInformation()
                                {
                                    Name = t1.Name,
                                    TransportAddress = t1.TransportAddress,
                                    Id = t1.Id,
                                    IsContactable = t1.IsContactable
                                });
                            }

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
                            Shards = t1.Shards.Select(s => s.DeepCopy()).ToList(),
                            Type = t1.Type
                        });
                        break;
                    case UpdateClusterTasks t1:
                        if (t1.TasksToAdd != null)
                        {
                            foreach (var task in t1.TasksToAdd)
                            {
                                if (GetRunningTask(task.UniqueRunningId) == null)
                                {
                                    if (!ClusterTasks.TryAdd(task.Id, SystemExtension.Clone(task)))
                                    {
                                        //Can't add a task twice
                                        if (ClusterTasks.ContainsKey(task.Id))
                                        {
                                            if (_logger == null)
                                                Console.WriteLine("Critical error while trying to add cluster task " + task.Id + " the id already exists as the object " + JsonConvert.SerializeObject(ClusterTasks[task.Id], Formatting.Indented));
                                            else
                                                _logger.LogDebug("Critical error while trying to add cluster task " + task.Id + " the id already exists as the object " + JsonConvert.SerializeObject(ClusterTasks[task.Id], Formatting.Indented));
                                        }
                                        else
                                        {
                                            throw new Exception("Critical error while trying to add cluster task " + task.Id);
                                        }
                                    }
                                }
                                else
                                {
                                    if (_logger == null)
                                        Console.WriteLine("The task already exists and is running. Skipping addition of task " + task.Id);
                                    else
                                        _logger.LogInformation("The task already exists and is running. Skipping addition of task " + task.Id);
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
                                if (ClusterTasks.ContainsKey(task.TaskId))
                                {
                                    ClusterTasks[task.TaskId].CompletedOn = task.CompletedOn;
                                    ClusterTasks[task.TaskId].Status = task.Status;
                                    ClusterTasks[task.TaskId].ErrorMessage = task.ErrorMessage;
                                }
                                else
                                {
                                    if (_logger == null)
                                        Console.WriteLine("Critical error while trying to update cluster task " + task.TaskId + " task is not present in dictionary.");
                                    else
                                        _logger.LogInformation("Critical error while trying to update cluster task " + task.TaskId + " task is not present in dictionary.");
                                }
                            }
                        }
                        break;
                    case UpdateShardMetadataAllocations t1:
                        if (t1.InsyncAllocationsToAdd != null)
                        {
                            var newList = new HashSet<Guid>();
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations.ToList().ForEach(ia => newList.Add(ia));
                            foreach (var allocation in t1.InsyncAllocationsToAdd)
                            {
                                //Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations = newList;
                                newList.Add(allocation);
                            }
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations = new HashSet<Guid>(newList);
                            // Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations = Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations.Where(sa => !t1.InsyncAllocationsToAdd.Contains(sa)).ToHashSet();
                        }
                        if (t1.InsyncAllocationsToRemove != null)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations = new HashSet<Guid>(Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations.Where(ia => !t1.InsyncAllocationsToRemove.Contains(ia)));
                        }
                        if (t1.StaleAllocationsToAdd != null)
                        {
                            var newList = new HashSet<Guid>();
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations.ToList().ForEach(ia => newList.Add(ia));
                            foreach (var allocation in t1.StaleAllocationsToAdd)
                            {
                                newList.Add(allocation);
                            }
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations = new HashSet<Guid>(newList);
                        }
                        if (t1.StaleAllocationsToRemove != null)
                        {
                            var newList = new HashSet<Guid>();
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations.Where(sa => !t1.StaleAllocationsToRemove.Contains(sa)).ToList().ForEach(ia => newList.Add(ia));
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations = new HashSet<Guid>(newList);
                        }

                        if (t1.LatestPos != null)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().LatestOperationPos = t1.LatestPos.Value;
                        }

                        if (t1.PrimaryAllocation != null)
                        {
                            Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().PrimaryAllocation = t1.PrimaryAllocation.Value;
                        }
                        break;
                    case SetLock t1:
                        var stringLockResult = Locks.TryAdd(t1.Name, new Lock()
                        {
                            LockTimeoutMs = t1.TimeoutMs,
                            Name = t1.Name,
                            LockId = t1.LockId,
                            CreatedOn = t1.CreatedOn
                        });
                        if (!stringLockResult)
                        {
                            throw new ConflictingObjectLockException("String " + t1.Name + " is already locked.");
                        }
                        break;
                    case RemoveLock t1:
                        string lockName = t1.Name;
                        var objectLocksWithLockId = Locks.Where(ob => ob.Value.LockId == t1.LockId).Select(t => t.Value);
                        if (objectLocksWithLockId.Count() == 0)
                        {
                            throw new ConflictingObjectLockException("No lock with id " + t1.LockId);
                        }

                        Lock existingStringLock;
                        var removeStringLockResult = Locks.TryRemove(t1.Name, out existingStringLock);
                        if (!removeStringLockResult)
                        {
                            throw new ConflictingObjectLockException("String Lock " + t1.Name + " did not exist in locks.");
                        }
                        break;
                    default:
                        ApplyCommandToState(command);
                        break;
                }
            }
            catch (Exception e)
            {
                throw new Exception("Failed to apply command " + command.CommandName + " to state with exception \"" + e.Message + "\"." + Environment.NewLine + JsonConvert.SerializeObject(command, Formatting.Indented));
            }
        }

        public abstract void ApplyCommandToState(BaseCommand command);
    }
}
