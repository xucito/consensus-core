using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Exceptions;
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
        public ConcurrentDictionary<string, Index> Indexes { get; set; } = new ConcurrentDictionary<string, Index>();
        public List<BaseTask> ClusterTasks { get; set; } = new List<BaseTask>();
        //object id and Shard id
        public ConcurrentDictionary<Guid, ObjectLock> ObjectLocks = new ConcurrentDictionary<Guid, ObjectLock>();

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
                        Nodes.Add(t1.Id, new NodeInformation()
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
                        Nodes.Remove(t1.Id);
                    }
                    break;
                case CreateIndex t1:
                    Indexes.TryAdd(t1.Type, new Index()
                    {
                        Shards = t1.Shards,
                        Type = t1.Type
                    });
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
                case UpdateShardMetadata t1:
                    if (t1.PrimaryAllocation != null)
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().PrimaryAllocation = t1.PrimaryAllocation;
                    if (t1.InsyncAllocations != null)
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().InsyncAllocations = t1.InsyncAllocations;
                    if (t1.StaleAllocations != null)
                        Indexes[t1.Type].Shards.Where(s => s.Id == t1.ShardId).FirstOrDefault().StaleAllocations = t1.StaleAllocations;
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
