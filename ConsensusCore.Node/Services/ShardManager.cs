using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Enums;
using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Connectors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services
{
    public class ShardManager<State, Repository>
        where State : BaseState, new()
        where Repository : IBaseRepository
    {
        public NodeStorage _nodeStorage { get; set; }
        ILogger<ShardManager<State, Repository>> Logger { get; set; }
        ClusterConnector _clusterConnector;
        IStateMachine<State> _stateMachine;
        IDataRouter _dataRouter;
        ClusterOptions _clusterOptions;

        public ShardManager(IStateMachine<State> stateMachine,
            Repository repository,
            ILogger<ShardManager<State, Repository>> logger,
            ClusterConnector connector,
            IDataRouter dataRouter,
            IOptions<ClusterOptions> clusterOptions,
            NodeStorage nodeStorage)
        {
            var storage = repository.LoadNodeData();
            _clusterConnector = connector;
            _stateMachine = stateMachine;
            _dataRouter = dataRouter;
            _clusterOptions = clusterOptions.Value;
            Logger = logger;
            _nodeStorage = nodeStorage;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="operation"></param>
        /// <param name="shardId"></param>
        /// <param name="type"></param>
        /// <param name="pos"></param>
        /// <param name="originalData">Only use in testing, shard manager should search this</param>
        public async void ReverseLocalTransaction(Guid shardId, string type, int pos, ShardData originalData = null)
        {
            ShardOperation operation = _nodeStorage.GetShardOperation(shardId, pos);
            try
            {
                Logger.LogWarning(" Reverting operation " + ":" + pos + " " + operation.Operation.ToString() + " on shard " + shardId + " for object " + operation.ObjectId);
                _nodeStorage.RemoveOperation(shardId, pos);
            }
            catch (ShardOperationConcurrencyException e)
            {
                Logger.LogError("Tried to remove a sync position out of order, " + e.Message + Environment.NewLine + e.StackTrace);
                throw e;
            }
            //Exception might be thrown because the sync position might be equal to the position you are setting
            catch (Exception e)
            {
                throw e;
            }

            ShardData data = originalData;
            if (data == null)
            {
                if (operation.Operation == ShardOperationOptions.Delete || operation.Operation == ShardOperationOptions.Update)
                {
                    var result = await _clusterConnector.Send(_stateMachine.GetShard(type, shardId).PrimaryAllocation, new RequestDataShard()
                    {
                        ShardId = shardId,
                        Type = type,
                        ObjectId = operation.ObjectId
                    });

                    data = result.Data;
                }
            }

            _nodeStorage.RevertedOperations.Add(new DataReversionRecord()
            {
                OriginalOperation = operation,
                NewData = data,
                OriginalData = await _dataRouter.GetDataAsync(type, operation.ObjectId),
                RevertedTime = DateTime.Now
            });

            switch (operation.Operation)
            {
                case ShardOperationOptions.Create:
                    data = await _dataRouter.GetDataAsync(type, operation.ObjectId);
                    if (data != null)
                    {
                        Logger.LogInformation("Reverting create with deletion of " + data);
                        await _dataRouter.DeleteDataAsync(data);
                    }
                    break;
                // Put the data back in the right position
                case ShardOperationOptions.Delete:
                    // Set correct data for null for a full rollback
                    if (data != null)
                        await _dataRouter.InsertDataAsync(data);
                    else
                    {
                        Logger.LogError("Failed to re-add deleted record " + operation.ObjectId + "...");
                        throw new Exception("Failed to re-add deleted record " + operation.ObjectId + "...");
                    }
                    break;
                case ShardOperationOptions.Update:
                    if (data != null)
                    {
                        if (data != null)
                            await _dataRouter.UpdateDataAsync(data);
                        else
                            await _dataRouter.InsertDataAsync(data);
                    }
                    else
                    {
                        Logger.LogError("Failed to revert update as there was no correct data given.");
                    }
                    break;
            }
        }

        public async Task<bool> RunDataOperation(ShardOperationOptions operation, ShardData shard)
        {
            try
            {
                switch (operation)
                {
                    case ShardOperationOptions.Create:
                        await _dataRouter.InsertDataAsync(shard);
                        break;
                    case ShardOperationOptions.Update:
                        if (!_nodeStorage.IsObjectMarkedForDeletion(shard.ShardId.Value, shard.Id))
                        {
                            try
                            {
                                await _dataRouter.UpdateDataAsync(shard);
                            }
                            catch (Exception e)
                            {
                                Logger.LogWarning("Failed to update data with exception " + e.Message + " trying to add the data instead.");
                                await _dataRouter.InsertDataAsync(shard);
                            }
                        }
                        else
                        {
                            Console.WriteLine("OBJECT IS MARKED FOR DELETION");
                        }
                        break;
                    case ShardOperationOptions.Delete:
                        if (_nodeStorage.MarkShardForDeletion(shard.ShardId.Value, shard.Id))
                        {
                            await _dataRouter.DeleteDataAsync(shard);
                        }
                        else
                        {
                            Logger.LogError("Ran into error while deleting " + shard.Id);
                            return false;
                        }
                        break;
                }
                return true;
            }
            catch (Exception e)
            {
                Logger.LogError("Failed to run data operation against shard " + shard.Id + " with exception " + e.StackTrace);
                return false;
            }
        }

        public void AddNewShardMetadata(Guid shardId, string shardType)
        {
            _nodeStorage.AddNewShardMetaData(new LocalShardMetaData()
            {
                ShardId = shardId,
                ShardOperations = new ConcurrentDictionary<int, ShardOperation>(),
                Type = shardType
            });
        }

        /// <summary>
        /// Sync the local shard with the clusters shard. Used mainly if the node is out of sync with the cluster.
        /// </summary>
        /// <param name="shardId"></param>
        /// <returns></returns>
        public async Task<bool> SyncShard(Guid shardId, string type)
        {
            LocalShardMetaData shard;
            //Check if shard exists, if not create it.
            if ((shard = _nodeStorage.GetShardMetadata(shardId)) == null)
            {
                //Create local empty shard
                _nodeStorage.AddNewShardMetaData(new LocalShardMetaData()
                {
                    ShardId = shardId,
                    ShardOperations = new ConcurrentDictionary<int, ShardOperation>(),
                    Type = type
                });
                shard = _nodeStorage.GetShardMetadata(shardId);
            }

            //you should sync twice, first to sync from stale, then to sync any trailing data
            var syncs = 0;
            Random rand = new Random();
            SharedShardMetadata shardMetadata = null;// = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
            Guid? selectedNode = null;
            var startTime = DateTime.Now;
            while (selectedNode == null)
            {
                shardMetadata = _stateMachine.GetShardMetadata(shard.ShardId, shard.Type);
                //Time out finding a allocation
                if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                {
                    Logger.LogError("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                    throw new ClusterOperationTimeoutException("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                }

                var randomlySelectedNode = shardMetadata.PrimaryAllocation;//shardMetadata.InsyncAllocations[rand.Next(0, shardMetadata.InsyncAllocations.Where(i => i != _nodeStorage.Id).Count())];

                if (_stateMachine.CurrentState.Nodes.ContainsKey(randomlySelectedNode) && _clusterConnector.ContainsNode(randomlySelectedNode))
                {
                    selectedNode = randomlySelectedNode;
                }
                else
                {
                    Logger.LogWarning(" primary node " + randomlySelectedNode + " was not available for recovery, sleeping and will try again...");
                    Thread.Sleep(1000);
                }
            }

            //Start by checking whether the last 3 transaction was consistent
            Logger.LogDebug(" rechecking consistency for shard " + shard.ShardId);

            if (shard.SyncPos != 0)
            {
                var isConsistent = false;
                var positionsToCheck = 20;
                while (!isConsistent)
                {
                    if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        Logger.LogError("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                        throw new ClusterOperationTimeoutException("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                    }

                    var pointToCheckFrom = shard.SyncPos < 2 ? shard.SyncPos : shard.SyncPos - positionsToCheck;
                    int? lowestPosition = null;

                    var from = shard.SyncPos < positionsToCheck ? 0 : shard.SyncPos - positionsToCheck;
                    var to = shard.SyncPos;

                    Logger.LogWarning(" pulling operation " + shard.ShardId + " from:" + from + " to:" + to);
                    var lastOperation = await _clusterConnector.Send(selectedNode.Value, new RequestShardOperations()
                    {
                        From = from,
                        To = to,
                        ShardId = shardId,
                        Type = type,
                        IncludeOperations = true
                    });

                    if (!lastOperation.IsSuccessful)
                    {
                        Logger.LogError(" could not successfully fetch operations for shard " + shard.ShardId + " from:" + from + " to:" + to);
                        throw new Exception(" could not successfully fetch operations." + shard.ShardId + " from:" + from + " to:" + to);
                    }

                    // If you have more operations then the primary, delete the local operations
                    var currentOperationCount = _nodeStorage.GetCurrentShardLatestCount(shard.ShardId);
                    if (lastOperation.LatestPosition < currentOperationCount)
                    {
                        Logger.LogWarning("Found local operations is ahead of primary, rolling back operations");
                        for (var i = currentOperationCount; i > lastOperation.LatestPosition; i--)
                        {
                            var operation = _nodeStorage.GetOperation(shardId, i);
                            ReverseLocalTransaction(shard.ShardId, shard.Type, i);
                        }
                    }

                    foreach (var pos in lastOperation.Operations.OrderByDescending(o => o.Key))
                    {
                        Logger.LogWarning("Checking operation " + pos.Key);
                        var myCopyOfTheTransaction = _nodeStorage.GetShardOperation(shard.ShardId, pos.Key);

                        if (myCopyOfTheTransaction == null || myCopyOfTheTransaction.ObjectId != pos.Value.ObjectId || myCopyOfTheTransaction.Operation != pos.Value.Operation)
                        {
                            Logger.LogWarning("Found my copy of the operation" + pos.Key + " was not equal to the primary. Reverting operation " + Environment.NewLine + JsonConvert.SerializeObject(myCopyOfTheTransaction));
                            //Reverse the transaction
                            ReverseLocalTransaction(shard.ShardId, shard.Type, pos.Key);
                            if (lowestPosition == null || lowestPosition > pos.Key)
                            {
                                lowestPosition = pos.Key;
                            }
                        }
                    }

                    Logger.LogError("My current operation count is " + currentOperationCount + " the remote operation count is " + lastOperation.LatestPosition);

                    if (lowestPosition == null)
                    {
                        isConsistent = true;
                        Logger.LogDebug("Detected three consistently transactions, continuing recovery for shard " + shard.ShardId);
                    }
                    else
                    {
                        Logger.LogWarning(" reverted sync position to " + _nodeStorage.GetShardSyncPositions()[shard.ShardId]);
                        shard = _nodeStorage.GetShardMetadata(shardId);
                    }
                }

            }


            var latestPrimary = await _clusterConnector.Send(selectedNode.Value, new RequestShardOperations()
            {
                From = shard.SyncPos + 1,
                To = shard.SyncPos + 1,
                ShardId = shardId,
                Type = type
            });

            var currentPos = _nodeStorage.GetShardMetadata(shardId).SyncPos;
            var lastPrimaryPosition = latestPrimary.LatestPosition;

            if (currentPos < lastPrimaryPosition)
            {
                while (currentPos < lastPrimaryPosition)
                {
                    if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        Logger.LogError("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                        throw new ClusterOperationTimeoutException("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                    }
                    var syncTo = lastPrimaryPosition > (shard.SyncPos + _clusterOptions.MaxObjectSync) ? (shard.SyncPos + _clusterOptions.MaxObjectSync) : lastPrimaryPosition;
                    Logger.LogDebug("Syncing from " + (shard.SyncPos + 1) + " to " + syncTo);
                    var nextOperations = await _clusterConnector.Send(selectedNode.Value, new RequestShardOperations()
                    {
                        From = shard.SyncPos + 1,
                        // do 100 records at a time
                        To = syncTo,
                        ShardId = shardId,
                        Type = type
                    });

                    foreach (var operation in nextOperations.Operations)
                    {
                        Logger.LogDebug("Recovering operation " + operation.Value.Position + " on shard " + shard.ShardId);

                        var replicationResult = await ReplicateShardOperation(shard.ShardId, new ShardOperation()
                        {
                            Operation = operation.Value.Operation,
                            ObjectId = operation.Value.ObjectId
                        }, (ShardData)operation.Value.Payload, operation.Key, type);

                        if (!replicationResult.IsSuccessful)
                        {
                            throw new Exception("Failed to replicate shard " + shard.ShardId + " operation " + operation.Value.Operation.ToString() + " for object " + operation.Value.ObjectId);
                        }
                        if (replicationResult.LatestPosition > lastPrimaryPosition)
                        {
                            Logger.LogDebug("Updating the sync length to " + lastPrimaryPosition + " for shard " + shard.ShardId);
                            lastPrimaryPosition = replicationResult.LatestPosition;
                        }
                    }

                }
            }
            Logger.LogDebug("Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
            return true;
        }

        public async Task<ReplicateShardOperationResponse> ReplicateShardOperation(Guid shardId, ShardOperation operation, ShardData payload, int pos, string type)
        {
            Logger.LogDebug("Recieved replication request for shard" + shardId + " for object " + operation.ObjectId + " for action " + operation.Operation.ToString() + " operation " + pos);
            var startTime = DateTime.Now;

            while (!_nodeStorage.CanApplyOperation(shardId, pos))
            {
                Thread.Sleep(100);
            }

            if (_nodeStorage.ReplicateShardOperation(shardId, pos, operation))
            {
                if (!await RunDataOperation(operation.Operation, payload))
                {
                    Logger.LogError("Ran into error while running operation " + operation.Operation.ToString() + " on " + shardId);
                    if (!_nodeStorage.RemoveOperation(shardId, pos))
                    {
                        Logger.LogError("Ran into critical error when rolling back operation " + pos + " on shard " + shardId);
                    }
                    return new ReplicateShardOperationResponse()
                    {
                        IsSuccessful = false
                    };
                }
                else
                {
                    _nodeStorage.MarkOperationAsCommited(shardId, pos);
                    Logger.LogDebug("Marked operation " + pos + " on shard " + shardId + "as commited");
                }
            }

            return new ReplicateShardOperationResponse()
            {
                IsSuccessful = true,
                LatestPosition = _nodeStorage.ShardMetaData[shardId].LatestShardOperation
            };
        }

        /// <summary>
        /// Write data assuming you are the primary allocation
        /// </summary>
        /// <param name="data"></param>
        /// <param name="operation"></param>
        /// <param name="waitForSafeWrite"></param>
        /// <param name="removeLock"></param>
        /// <returns></returns>
        public async Task<WriteDataResponse> WriteData(ShardData data, ShardOperationOptions operation, bool waitForSafeWrite, bool removeLock = false)
        {
            SharedShardMetadata shardMetadata = _stateMachine.GetShard(data.ShardType, data.ShardId.Value);
            //Write to the replicated nodes
            ConcurrentBag<Guid> InvalidNodes = new ConcurrentBag<Guid>();

            if (_nodeStorage.GetShardMetadata(data.ShardId.Value) == null)
            {
                Logger.LogDebug("Creating local copy of shard " + data.ShardId);
                AddNewShardMetadata(data.ShardId.Value, data.ShardType);
            }

            //Commit the sequence Number
            int sequenceNumber = _nodeStorage.AddNewShardOperation(data.ShardId.Value, new ShardOperation()
            {
                ObjectId = data.Id,
                Operation = operation
            });

            if (!await RunDataOperation(operation, data))
            {
                Logger.LogError("Ran into error while running operation " + operation.ToString() + " on " + data.Id);
                if (!_nodeStorage.RemoveOperation(data.ShardId.Value, sequenceNumber))
                {
                    Logger.LogError("Ran into critical error when rolling back operation " + sequenceNumber + " on shard " + data.ShardId.Value);
                }
                return new WriteDataResponse()
                {
                    IsSuccessful = false
                };
            }
            else
            {
                //If the shard metadata is not synced upto date
                if (_nodeStorage.GetShardMetadata(data.ShardId.Value).SyncPos < sequenceNumber - 1)
                {
                    //Logger.LogInformation("Detected delayed sync position, sending recovery command.");
                    //AddShardSyncTask(data.Id, data.ShardType);
                }
                _nodeStorage.MarkOperationAsCommited(data.ShardId.Value, sequenceNumber);
                //All allocations except for your own
                var tasks = shardMetadata.InsyncAllocations.Where(id => id != _nodeStorage.Id).Select(async allocation =>
                {
                    try
                    {
                        var result = await _clusterConnector.Send(allocation, new ReplicateShardOperation()
                        {
                            ShardId = shardMetadata.Id,
                            Operation = new ShardOperation()
                            {
                                ObjectId = data.Id,
                                Operation = operation
                            },
                            Payload = data,
                            Pos = sequenceNumber,
                            Type = data.ShardType
                        });

                        if (result.IsSuccessful)
                        {
                            Logger.LogDebug("Successfully replicated all " + shardMetadata.Id + "shards.");
                        }
                        else
                        {
                            throw new Exception("Failed to replicate data to shard " + shardMetadata.Id + " to node " + allocation);
                        }
                    }
                    catch (TaskCanceledException e)
                    {
                        Logger.LogError("Failed to replicate shard " + shardMetadata.Id + " on shard " + _stateMachine.CurrentState.Nodes[allocation].TransportAddress + " for operation " + sequenceNumber + " as request timed out, marking shard as not insync...");
                        InvalidNodes.Add(allocation);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError("Failed to replicate shard " + shardMetadata.Id + " for operation " + sequenceNumber + ", marking shard as not insync..." + e.StackTrace);
                        InvalidNodes.Add(allocation);
                    }
                });

                await Task.WhenAll(tasks);
                return new WriteDataResponse()
                {
                    IsSuccessful = true,
                    FailedNodes = InvalidNodes.ToList(),
                    ShardId = shardMetadata.Id,
                    Pos = sequenceNumber
                };

            }
        }

        public async Task<RequestShardOperationsResponse> RequestShardOperations(Guid shardId, int from, int to, string type, bool includeOperations)
        {
            //Check that the shard is insync here
            var localShard = _nodeStorage.GetShardMetadata(shardId);

            if (localShard == null)
            {
                Logger.LogError("Request for shard " + shardId + " however shard does not exist on node.");
                return new RequestShardOperationsResponse()
                {
                    IsSuccessful = false
                };
            }
            SortedDictionary<int, ShardOperationMessage> FinalList = new SortedDictionary<int, ShardOperationMessage>();


            if (includeOperations)
            {
                //foreach value in from - to, pull the operation and then pull the object from object router
                for (var i = from; i <= to; i++)
                {
                    var operation = _nodeStorage.GetOperation(shardId, i);
                    if (operation != null)
                    {
                        //This data could be in a future state
                        var currentData = (ShardData)await _dataRouter.GetDataAsync(type, operation.ObjectId);
                        FinalList.Add(i, new ShardOperationMessage()
                        {
                            ObjectId = operation.ObjectId,
                            Operation = operation.Operation,
                            Payload = currentData,
                            Position = i
                        });
                    }
                }
            }

            return new RequestShardOperationsResponse()
            {
                IsSuccessful = true,
                LatestPosition = _nodeStorage.GetCurrentShardLatestCount(shardId),
                Operations = FinalList
            };
        }

        public async Task<RequestDataShardResponse> RequestDataShard(Guid objectId, string type, int timeoutMs, Guid? shardId = null)
        {
            Guid? FoundShard = null;
            Guid? FoundOnNode = null;
            var currentTime = DateTime.Now;

            if (shardId == null)
            {
                var shards = _stateMachine.GetAllPrimaryShards(type);

                bool foundResult = false;
                ShardData finalObject = null;
                var totalRespondedShards = 0;

                var tasks = shards.Select(async shard =>
                {
                    if (shard.Value != _nodeStorage.Id)
                    {
                        try
                        {
                            var result = await _clusterConnector.Send(shard.Value, new RequestDataShard()
                            {
                                ObjectId = objectId,
                                ShardId = shard.Key, //Set the shard
                                Type = type
                            });

                            if (result.IsSuccessful)
                            {
                                foundResult = true;
                                finalObject = result.Data;
                                FoundShard = result.ShardId;
                                FoundOnNode = result.NodeId;
                            }

                            Interlocked.Increment(ref totalRespondedShards);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError("Error thrown while getting " + e.Message);
                        }
                    }
                    else
                    {
                        finalObject = await _dataRouter.GetDataAsync(type, objectId);
                        foundResult = finalObject != null ? true : false;
                        FoundShard = shard.Key;
                        FoundShard = shard.Value;
                        Interlocked.Increment(ref totalRespondedShards);
                    }
                });

                //Don't await, this will trigger the tasks
                Task.WhenAll(tasks);

                while (!foundResult && totalRespondedShards < shards.Count)
                {
                    if ((DateTime.Now - currentTime).TotalMilliseconds > timeoutMs)
                    {
                        return new RequestDataShardResponse()
                        {
                            IsSuccessful = false
                        };
                    }
                    Thread.Sleep(10);
                }

                return new RequestDataShardResponse()
                {
                    IsSuccessful = true,
                    Data = finalObject,
                    SearchMessage = finalObject != null ? null : "Object " + objectId + " could not be found in shards.",

                };
            }
            else
            {
                var finalObject = await _dataRouter.GetDataAsync(type, objectId);
                return new RequestDataShardResponse()
                {
                    IsSuccessful = finalObject != null,
                    Data = finalObject,
                    NodeId = FoundOnNode,
                    ShardId = FoundShard,
                    SearchMessage = finalObject != null ? null : "Object " + objectId + " could not be found in shards.",
                };
            }
        }

    }
}
