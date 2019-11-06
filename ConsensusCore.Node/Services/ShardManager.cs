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
    public class ShardManager<State, ShardRepository>
        where State : BaseState, new()
        where ShardRepository : IShardRepository
    {

        //public NodeStorage<State> _nodeStorage { get; set; }
        ILogger<ShardManager<State, ShardRepository>> Logger { get; set; }
        ClusterConnector _clusterConnector;
        IStateMachine<State> _stateMachine;
        IDataRouter _dataRouter;
        ClusterOptions _clusterOptions;
        IShardRepository _shardRepository;
        Guid _nodeId;
        public string LogPrefix = "";

        //public ConcurrentDictionary<Guid, LocalShardMetaData> ShardMetaData { get; set; } = new ConcurrentDictionary<Guid, LocalShardMetaData>();
        /// <summary>
        /// A lock for every shard operation
        /// </summary>
        public ConcurrentDictionary<Guid, object> ShardOperationLocks { get; set; } = new ConcurrentDictionary<Guid, object>();

        public ShardManager(IStateMachine<State> stateMachine,
            ILogger<ShardManager<State, ShardRepository>> logger,
            ClusterConnector connector,
            IDataRouter dataRouter,
            IOptions<ClusterOptions> clusterOptions,
            // NodeStorage<State> nodeStorage,
            IShardRepository shardRepository,
            string logPrefix = "")
        {
            LogPrefix = logPrefix;
            _clusterConnector = connector;
            _stateMachine = stateMachine;
            _dataRouter = dataRouter;
            _clusterOptions = clusterOptions.Value;
            Logger = logger;
            _shardRepository = shardRepository;
        }

        public void SetNodeId(Guid id)
        {
            _nodeId = id;
        }

        public void GetOrAddLock(Guid objectId)
        {
            if (!ShardOperationLocks.ContainsKey(objectId))
            {
                ShardOperationLocks.TryAdd(objectId, new object());
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="operation"></param>
        /// <param name="shardId"></param>
        /// <param name="type"></param>
        /// <param name="pos"></param>
        /// <param name="originalData">Only use in testing, shard manager should search this</param>
        public async void ReverseLocalTransaction(Guid shardId, string type, int revertToPos, Dictionary<Guid, ShardData> originalData)
        {
            var shardMetadata = _shardRepository.GetShardMetadata(shardId);

            var pos = _shardRepository.GetTotalShardOperationsCount(shardId);
            while (pos >= revertToPos)
            {
                ShardOperation operation = _shardRepository.GetShardOperation(shardId, pos);
                if (operation != null)
                {
                    try
                    {
                        Logger.LogWarning(LogPrefix + " Reverting operation " + ":" + pos + " " + operation.Operation.ToString() + " on shard " + shardId + " for object " + operation.ObjectId);
                        _shardRepository.RemoveShardOperation(shardId, pos);

                    }
                    catch (ShardOperationConcurrencyException e)
                    {
                        Logger.LogError(LogPrefix + "Tried to remove a sync position out of order, " + e.Message + Environment.NewLine + e.StackTrace);
                        throw e;
                    }
                    //Exception might be thrown because the sync position might be equal to the position you are setting
                    catch (Exception e)
                    {
                        throw e;
                    }

                    if (!originalData.ContainsKey(operation.ObjectId))
                    {
                        if (operation.Operation == ShardOperationOptions.Delete || operation.Operation == ShardOperationOptions.Update)
                        {
                            Logger.LogError("Failed to revert operation " + pos + " as the original record was not provided for object " + operation.ObjectId + ".");
                        }
                        originalData.Add(operation.ObjectId, null);
                    }

                    ShardData data = originalData[operation.ObjectId];

                    _shardRepository.AddDataReversionRecord(new DataReversionRecord()
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
                                Logger.LogInformation(LogPrefix + "Reverting create with deletion of " + data);
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
                                Logger.LogError(LogPrefix + "Failed to re-add deleted record " + operation.ObjectId + "... It is likely the entire record is being reverted");
                                //throw new Exception("Failed to re-add deleted record " + operation.ObjectId + "...");
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
                                Logger.LogError(LogPrefix + "Failed to revert update as there was no correct data given... It is likely the entire record is being reverted");
                            }
                            break;
                    }
                }
                RevertShardSyncPos(shardId, pos);
                pos--;
            }
        }

        private bool RevertShardSyncPos(Guid shardId, int revertedPos)
        {
            var shardMetadata = _shardRepository.GetShardMetadata(shardId);
            if (revertedPos == shardMetadata.SyncPos)
            {
                shardMetadata.SyncPos--;
                return _shardRepository.UpdateShardMetadata(shardMetadata);
            }
            //This would have been a uncommited operation
            else if (revertedPos > shardMetadata.SyncPos)
            {
                return true;
            }
            else
            {
                throw new ShardOperationConcurrencyException("Failed to revert shard sync position as current sync position is " + shardMetadata.SyncPos + " and you are attempting to revert position " + revertedPos);
            }
        }

        public LocalShardMetaData GetShardLocalMetadata(Guid shardId)
        {
            return _shardRepository.GetShardMetadata(shardId);
        }

        public IEnumerable<ShardOperation> GetShardOperations(Guid shardId)
        {
            return _shardRepository.GetAllShardOperations(shardId);
        }

        public async Task<bool> RunDataOperation(ShardOperationOptions operation, ShardData data)
        {
            try
            {
                switch (operation)
                {
                    // Note that a key assumption is data with the same GUID is the same piece of data.
                    case ShardOperationOptions.Create:
                        await _dataRouter.InsertDataAsync(data);
                        break;
                    case ShardOperationOptions.Update:
                        if (!_shardRepository.IsObjectMarkedForDeletion(data.ShardId.Value, data.Id))
                        {
                            try
                            {
                                await _dataRouter.UpdateDataAsync(data);
                            }
                            catch (Exception e)
                            {
                                Logger.LogWarning(LogPrefix + "Failed to update data with exception " + e.Message + " trying to add the data instead.");
                                await _dataRouter.InsertDataAsync(data);
                            }
                        }
                        else
                        {
                            Console.WriteLine("OBJECT IS MARKED FOR DELETION");
                        }
                        break;
                    case ShardOperationOptions.Delete:
                        if (_shardRepository.MarkObjectForDeletion(new ObjectDeletionMarker()
                        {
                            GeneratedOn = DateTime.UtcNow,
                            ObjectId = data.Id,
                            ShardId = data.ShardId.Value
                        }))
                        {
                            await _dataRouter.DeleteDataAsync(data);
                        }
                        else
                        {
                            Logger.LogError("Ran into error while deleting " + data.Id);
                            return false;
                        }
                        break;
                }
                return true;
            }
            catch (Exception e)
            {
                Logger.LogError(LogPrefix + "Failed to run data operation against shard " + data.Id + " with exception " + e.Message + Environment.NewLine + e.StackTrace);
                return false;
            }
        }

        public void AddNewShardMetadata(Guid shardId, string shardType)
        {
            _shardRepository.AddNewShardMetadata(new LocalShardMetaData()
            {
                ShardId = shardId,
                Type = shardType
            });
        }

        public async void RemoveAllUnappliedOperations(Guid shardId, string type)
        {
            var allUncommitedOperations = _shardRepository.GetAllUncommitedOperations(shardId);
            foreach (var operation in allUncommitedOperations)
            {
                Logger.LogWarning(LogPrefix + "Removing uncommited operation " + operation.Pos + " on shard " + shardId);
                _shardRepository.RemoveShardOperation(shardId, operation.Pos);
                if (operation.Operation == ShardOperationOptions.Create)
                {
                    var data = await _dataRouter.GetDataAsync(type, operation.ObjectId);
                    if (data != null && data.ShardId == shardId)
                    {
                        Logger.LogError(LogPrefix + " removing trailing data from database " + operation.ObjectId + Environment.NewLine + JsonConvert.SerializeObject(data, Formatting.Indented));
                        await _dataRouter.DeleteDataAsync(data);
                    }
                }
            }
        }

        /// <summary>
        /// Sync the local shard with the clusters shard. Used mainly if the node is out of sync with the cluster.
        /// </summary>
        /// <param name="shardId"></param>
        /// <returns></returns>
        public async Task<bool> SyncShard(Guid shardId, string type, int positionsToCheck)
        {
            RemoveAllUnappliedOperations(shardId, type);

            LocalShardMetaData shard;
            //Check if shard exists, if not create it.
            if (!_shardRepository.ShardMetadataExists(shardId))
            {
                //Create local empty shard
                _shardRepository.AddNewShardMetadata(new LocalShardMetaData()
                {
                    ShardId = shardId,
                    Type = type
                });
            }
            shard = _shardRepository.GetShardMetadata(shardId);

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
                    Logger.LogError(LogPrefix + "Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
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
            Logger.LogDebug(LogPrefix + " rechecking consistency for shard " + shard.ShardId);

            if (shard.SyncPos != 0)
            {
                var isConsistent = false;
                while (!isConsistent)
                {
                    if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                    {
                        Logger.LogError(LogPrefix + "Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                        throw new ClusterOperationTimeoutException("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                    }

                    var pointToCheckFrom = shard.SyncPos < 2 ? shard.SyncPos : shard.SyncPos - positionsToCheck;
                    int? lowestPosition = null;

                    var from = shard.SyncPos < positionsToCheck ? 1 : shard.SyncPos - positionsToCheck;
                    var to = shard.SyncPos < from ? from : shard.SyncPos;

                    Logger.LogWarning(LogPrefix + " pulling operation " + shard.ShardId + " from:" + from + " to:" + to);
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
                        Logger.LogError(LogPrefix + " could not successfully fetch operations for shard " + shard.ShardId + " from:" + from + " to:" + to);
                        throw new Exception(" could not successfully fetch operations." + shard.ShardId + " from:" + from + " to:" + to);
                    }

                    // If you have more operations then the primary, delete the local operations
                    var currentOperationCount = _shardRepository.GetTotalShardOperationsCount(shard.ShardId);
                    Dictionary<Guid, ShardData> correctDataset = new Dictionary<Guid, ShardData>();

                    if (lastOperation.LatestPosition < currentOperationCount)
                    {
                        Logger.LogWarning(LogPrefix + "Found local operations is ahead of primary, rolling back operations. Local transactions is " + currentOperationCount + " remote operations is " + lastOperation.LatestPosition);

                        for (var i = currentOperationCount; i > lastOperation.LatestPosition; i--)
                        {
                            var operation = _shardRepository.GetShardOperation(shardId, i);
                            if (!correctDataset.ContainsKey(operation.ObjectId))
                            {
                                correctDataset.Add(operation.ObjectId, await GetOriginalDataForOperationReversal(operation.Operation, operation.ObjectId, operation.ShardId, type));
                            }
                        }
                        lowestPosition = lastOperation.LatestPosition;
                    }

                    Logger.LogInformation(LogPrefix + "Checking operations " + lastOperation.Operations.First().Key + " to " + lastOperation.Operations.Last());

                    foreach (var pos in lastOperation.Operations.Where(o => o.Key <= shard.SyncPos).OrderByDescending(o => o.Key))
                    {
                        Logger.LogInformation(LogPrefix + "Checking operation " + pos.Key);
                        var myCopyOfTheTransaction = _shardRepository.GetShardOperation(shard.ShardId, pos.Key);
                        if (myCopyOfTheTransaction == null || !myCopyOfTheTransaction.Equals(pos.Value.ShardOperation))
                        {
                            Logger.LogWarning(LogPrefix + "Found my copy of the operation" + pos.Key + " was not equal to the primary. Marking operation " + pos.Key + " for reversion." + Environment.NewLine + JsonConvert.SerializeObject(myCopyOfTheTransaction) + Environment.NewLine + JsonConvert.SerializeObject(pos.Value.ShardOperation));
                            if (myCopyOfTheTransaction != null && !correctDataset.ContainsKey(myCopyOfTheTransaction.ObjectId))
                            {
                                correctDataset.Add(myCopyOfTheTransaction.ObjectId, await GetOriginalDataForOperationReversal(myCopyOfTheTransaction.Operation, myCopyOfTheTransaction.ObjectId, shardId, type));
                            }
                            if (lowestPosition == null || lowestPosition > pos.Key)
                            {
                                lowestPosition = pos.Key;
                            }
                        }
                    }

                    if (lowestPosition == null)
                    {
                        isConsistent = true;
                        Logger.LogDebug(LogPrefix + "Detected three consistently transactions, continuing recovery for shard " + shard.ShardId);
                    }
                    else
                    {
                        for (var i = currentOperationCount; i >= lowestPosition; i--)
                        {
                            var operation = _shardRepository.GetShardOperation(shardId, i);
                            if (!correctDataset.ContainsKey(operation.ObjectId))
                            {
                                correctDataset.Add(operation.ObjectId, await GetOriginalDataForOperationReversal(operation.Operation, operation.ObjectId, shardId, type));
                            }
                        }

                        ReverseLocalTransaction(shard.ShardId, shard.Type, lowestPosition.Value, correctDataset);
                        shard = _shardRepository.GetShardMetadata(shardId);
                        Logger.LogWarning(LogPrefix + "Reverted sync position to sync position " + shard.SyncPos + " with number of operations " + _shardRepository.GetTotalShardOperationsCount(shard.ShardId) + "for shard " + shard.ShardId);

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

            var lastPrimaryPosition = latestPrimary.LatestPosition;

            var currentShardPosition = 0;
            while ((currentShardPosition = _shardRepository.GetShardMetadata(shardId).SyncPos) < lastPrimaryPosition)
            {
                if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                {
                    Logger.LogError(LogPrefix + "Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                    throw new ClusterOperationTimeoutException("Failed to completed sync shard for shard " + shardId + " for type " + type + " request timed out...");
                }
                var syncTo = lastPrimaryPosition > (currentShardPosition + _clusterOptions.MaxObjectSync) ? (currentShardPosition + _clusterOptions.MaxObjectSync) : lastPrimaryPosition;
                Logger.LogInformation(LogPrefix + "Syncing from " + (currentShardPosition + 1) + " to " + syncTo + " current sync position is " + currentShardPosition + " and last primary position is " + lastPrimaryPosition);
                var nextOperations = await _clusterConnector.Send(selectedNode.Value, new RequestShardOperations()
                {
                    From = currentShardPosition + 1,
                    // do 100 records at a time
                    To = syncTo,
                    ShardId = shardId,
                    Type = type
                });

                foreach (var operation in nextOperations.Operations)
                {
                    Logger.LogDebug(LogPrefix + "Recovering operation " + operation.Value.Position + " on shard " + shard.ShardId);

                    var replicationResult = await ReplicateShardOperation(new ShardOperation()
                    {
                        Operation = operation.Value.Operation,
                        ObjectId = operation.Value.ObjectId,
                        ShardId = shard.ShardId,
                        Pos = operation.Key
                    }, (ShardData)operation.Value.Payload);

                    if (!replicationResult.IsSuccessful)
                    {
                        throw new Exception("Failed to replicate shard " + shard.ShardId + " operation " + operation.Value.Operation.ToString() + " for object " + operation.Value.ObjectId);
                    }
                    if (replicationResult.LatestPosition > lastPrimaryPosition)
                    {
                        Logger.LogDebug(LogPrefix + "Updating the sync length to " + lastPrimaryPosition + " for shard " + shard.ShardId);
                        lastPrimaryPosition = replicationResult.LatestPosition;
                    }
                }
            }
            Logger.LogDebug(LogPrefix + "Caught up shard " + shard.ShardId + ". Reallocating node as insync and creating watch period.");
            return true;
        }

        public async Task<ShardData> GetOriginalDataForOperationReversal(ShardOperationOptions typeOfOperation, Guid objectId, Guid shardId, string type)
        {
            if (typeOfOperation == ShardOperationOptions.Delete || typeOfOperation == ShardOperationOptions.Update)
            {
                return (await _clusterConnector.Send(_stateMachine.GetShard(type, shardId).PrimaryAllocation, new RequestDataShard()
                {
                    ShardId = shardId,
                    Type = type,
                    ObjectId = objectId
                })).Data;
            }
            return null;
        }

        public async Task<ReplicateShardOperationResponse> ReplicateShardOperation(ShardOperation operation, ShardData payload)
        {
            Logger.LogDebug(LogPrefix + "Recieved replication request for shard" + operation.ShardId + " for object " + operation.ObjectId + " for action " + operation.Operation.ToString() + " operation " + operation.Pos);
            var startTime = DateTime.Now;
            operation.Applied = false;

            while (!CanApplyOperation(operation.ShardId, operation.Pos))
            {
                if ((DateTime.Now - startTime).TotalMilliseconds > _clusterOptions.DataTransferTimeoutMs)
                {
                    throw new ClusterOperationTimeoutException("Failed to complete operation as not is unable to apply operation " + operation.Pos + " on shard " + operation.ShardId);
                }
                Thread.Sleep(100);
            }

            bool successfullyAddedOperation = false;

            GetOrAddLock(operation.ShardId);
            lock (ShardOperationLocks[operation.ShardId])
            {
                successfullyAddedOperation = _shardRepository.AddShardOperation(operation);
            }

            if (successfullyAddedOperation)
            {
                if (!await RunDataOperation(operation.Operation, payload))
                {
                    Logger.LogError(LogPrefix + "Ran into error while running operation " + operation.Operation.ToString() + " on " + operation.ShardId);
                    if (!_shardRepository.RemoveShardOperation(operation.ShardId, operation.Pos))
                    {
                        Logger.LogError(LogPrefix + "Ran into critical error when rolling back operation " + operation.Pos + " on shard " + operation.ShardId);
                    }
                    return new ReplicateShardOperationResponse()
                    {
                        IsSuccessful = false
                    };
                }
                else
                {
                    operation.Applied = true;
                    _shardRepository.UpdateShardOperation(operation.ShardId, operation);

                    var shard = _shardRepository.GetShardMetadata(operation.ShardId);
                    shard.SyncPos = operation.Pos;
                    _shardRepository.UpdateShardMetadata(shard);

                    Logger.LogDebug(LogPrefix + "Marked operation " + operation.Pos + " on shard " + operation.ShardId + "as commited");
                }
            }

            return new ReplicateShardOperationResponse()
            {
                IsSuccessful = true,
                LatestPosition = _shardRepository.GetTotalShardOperationsCount(operation.ShardId)
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

            if (_shardRepository.GetShardMetadata(data.ShardId.Value) == null)
            {
                Logger.LogDebug(LogPrefix + "Creating local copy of shard " + data.ShardId);
                AddNewShardMetadata(data.ShardId.Value, data.ShardType);
            }

            //Commit the sequence Number
            var submittedOperation = AddShardOperation(new ShardOperation()
            {
                ObjectId = data.Id,
                Operation = operation,
                ShardId = data.ShardId.Value,
                Applied = false
            });

            if (!await RunDataOperation(operation, data))
            {
                Logger.LogError(LogPrefix + "Ran into error while running operation " + operation.ToString() + " on " + data.Id);
                if (!_shardRepository.RemoveShardOperation(data.ShardId.Value, submittedOperation.Pos))
                {
                    Logger.LogError(LogPrefix + "Ran into critical error when rolling back operation " + submittedOperation.Pos + " on shard " + data.ShardId.Value);
                }
                return new WriteDataResponse()
                {
                    IsSuccessful = false
                };
            }
            else
            {
                //If the shard metadata is not synced upto date
                if (_shardRepository.GetShardMetadata(data.ShardId.Value).SyncPos < submittedOperation.Pos - 1)
                {
                    //Logger.LogInformation("Detected delayed sync position, sending recovery command.");
                    //AddShardSyncTask(data.Id, data.ShardType);
                }

                submittedOperation.Applied = true;
                _shardRepository.UpdateShardOperation(submittedOperation.ShardId, submittedOperation);

                var shard = _shardRepository.GetShardMetadata(submittedOperation.ShardId);
                shard.SyncPos = submittedOperation.Pos;
                _shardRepository.UpdateShardMetadata(shard);
                //All allocations except for your own
                var tasks = shardMetadata.InsyncAllocations.Where(id => id != _nodeId).Select(async allocation =>
                {
                    try
                    {
                        var result = await _clusterConnector.Send(allocation, new ReplicateShardOperation()
                        {
                            Operation = new ShardOperation()
                            {
                                ObjectId = data.Id,
                                Operation = operation,
                                ShardId = shardMetadata.Id,
                                Pos = submittedOperation.Pos
                            },
                            Payload = data,
                            Type = data.ShardType
                        });

                        if (result.IsSuccessful)
                        {
                            Logger.LogDebug(LogPrefix + "Successfully replicated all " + shardMetadata.Id + "shards.");
                        }
                        else
                        {
                            throw new Exception("Failed to replicate data to shard " + shardMetadata.Id + " to node " + allocation);
                        }
                    }
                    catch (TaskCanceledException e)
                    {
                        Logger.LogError(LogPrefix + "Failed to replicate shard " + shardMetadata.Id + " on shard " + _stateMachine.CurrentState.Nodes[allocation].TransportAddress + " for operation " + submittedOperation.Pos + " as request timed out, marking shard as not insync...");
                        InvalidNodes.Add(allocation);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(LogPrefix + "Failed to replicate shard " + shardMetadata.Id + " for operation " + submittedOperation.Pos + ", marking shard as not insync..." + e.StackTrace);
                        InvalidNodes.Add(allocation);
                    }
                });

                await Task.WhenAll(tasks);
                return new WriteDataResponse()
                {
                    IsSuccessful = true,
                    FailedNodes = InvalidNodes.ToList(),
                    ShardId = shardMetadata.Id,
                    Pos = submittedOperation.Pos
                };

            }
        }

        public async Task<RequestShardOperationsResponse> RequestShardOperations(Guid shardId, int from, int to, string type, bool includeOperations)
        {
            //Check that the shard is insync here
            var localShard = _shardRepository.GetShardMetadata(shardId);

            if (localShard == null)
            {
                Logger.LogError(LogPrefix + "Request for shard " + shardId + " however shard does not exist on node.");
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
                    var operation = _shardRepository.GetShardOperation(shardId, i);
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
                LatestPosition = _shardRepository.GetTotalShardOperationsCount(shardId),
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
                    if (shard.Value != _nodeId)
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
                            Logger.LogError(LogPrefix + "Error thrown while getting " + e.Message);
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

        public ShardOperation AddShardOperation(ShardOperation operation)
        {
            int newOperationId;
            GetOrAddLock(operation.ShardId);
            lock (ShardOperationLocks[operation.ShardId])
            {
                newOperationId = _shardRepository.GetTotalShardOperationsCount(operation.ShardId) + 1;
                operation.Pos = newOperationId;
                bool addResult = _shardRepository.AddShardOperation(operation);
                if (!addResult)
                {
                    Console.WriteLine("ERROR:Failed to add operation!!!");
                    throw new Exception("Failed to add shard operation to disk.");
                }
            }

            operation.Pos = newOperationId;
            return operation;
        }

        public bool IsObjectMarkedForDeletion(Guid shardId, Guid objectId)
        {
            return _shardRepository.IsObjectMarkedForDeletion(shardId, objectId);
        }

        /*public void MarkShardAsApplied(Guid shardId, int pos)
        {
            _shardRepository.MarkShardOperationAsApplied(shardId, pos);
            GetOrAddLock(shardId);
            lock (ShardOperationLocks[shardId])
            {
                var shard = _shardRepository.GetShardMetadata(shardId);
                if (pos > shard.SyncPos)
                {
                    //Console.WriteLine("Updated sync position with " + pos + "from sync pos" + SyncPos);
                    shard.SyncPos = pos;
                    _shardRepository.UpdateShardMetadata(shard);
                }
            }
        }*/


        public bool CanApplyOperation(Guid shardId, int pos)
        {
            if (pos == 1)
            {
                return true;
            }

            var operation = _shardRepository.GetShardOperation(shardId, pos - 1);

            if (operation == null || !operation.Applied)
            {
                return false;
            }
            return true;
        }


        /// <summary>
        /// Only used when the operation was recorded but not applied to datastore
        /// </summary>
        /// <returns></returns>
       /* public bool RemoveOperation(Guid shardId, int pos)
        {
            var shard = _shardRepository.GetShardMetadata(shardId);
            if (shard.SyncPos <= pos)
            {
                GetOrAddLock(shardId);
                lock (ShardOperationLocks[shardId])
                {
                    if (!_shardRepository.RemoveShardOperation(shardId, pos))
                    {
                        //throw new ShardOperationConcurrencyException("Failed to remove the operation" + pos + " from shard " + ShardId);
                    }
                    //If pos the last sync position, set the position back one
                    if (shard.SyncPos == pos)
                    {
                        shard.SyncPos--;
                        _shardRepository.UpdateShardMetadata(shard);
                    }
                }
                return true;
            }
            throw new ShardOperationConcurrencyException("CONCURRENCY error while trying to reverse operation, the sync position is " + shard.SyncPos + " and the position to remove is " + pos + ".");
        }*/

        public bool MarkObjectForDeletion(Guid shardId, Guid objectId)
        {
            return _shardRepository.MarkObjectForDeletion(new ObjectDeletionMarker()
            {
                GeneratedOn = DateTime.UtcNow,
                ShardId = shardId
            });
        }


        public bool ObjectIsMarkedForDeletion(Guid shardId, Guid objectId)
        {
            return _shardRepository.IsObjectMarkedForDeletion(shardId, objectId);
        }

        public int GetTotalShardOperationCount(Guid shardId)
        {
            return _shardRepository.GetTotalShardOperationsCount(shardId);
        }
    }
}
