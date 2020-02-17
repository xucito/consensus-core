using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Services;
using ConsensusCore.Domain.Utility;
using ConsensusCore.Node.Utility;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Repositories
{
    public partial class NodeInMemoryRepository<Z> : IOperationCacheRepository
        where Z : BaseState, new()
    {
        public Task<bool> AddOperationToTransitAsync(ShardWriteOperation operation)
        {
            return Task.FromResult(TransitQueue.TryAdd(operation.Id, SystemExtension.Clone(operation)));
        }

        public int CountOperationsInQueue()
        {
            return OperationQueue.Count();
        }

        public int CountOperationsInTransit()
        {
            return TransitQueue.Count();
        }

        public Task<bool> DeleteOperationFromQueueAsync(ShardWriteOperation operation)
        {
            lock (queueLock)
            {
                OperationQueue.Remove(operation);
            }
            return Task.FromResult(true);
        }

        public Task<bool> DeleteOperationFromTransitAsync(string transactionId)
        {
            return Task.FromResult(TransitQueue.Remove(transactionId));
        }

        public Task<bool> EnqueueOperationAsync(ShardWriteOperation data)
        {
            lock (queueLock)
            {
                OperationQueue.Add(SystemExtension.Clone(data));
            }
            return Task.FromResult(true);
        }

        public Task<ShardWriteOperation> GetNextOperationAsync()
        {
            lock (queueLock)
            {
                var result = OperationQueue.Take(1);
                if (result.Count() > 0)
                {
                    return Task.FromResult(result.First());
                }
            }
            return Task.FromResult<ShardWriteOperation>(null);
        }

        public async Task<IEnumerable<ShardWriteOperation>> GetOperationQueueAsync()
        {
            return OperationQueue;
        }

        public async Task<IEnumerable<ShardWriteOperation>> GetTransitQueueAsync()
        {
            return TransitQueue.Values;
        }

        public bool IsOperationInQueue(string operationId)
        {
            lock (queueLock)
            {
                return OperationQueue.ToList().Find(oq => oq.Id == operationId) != null;
            }
        }

        public bool IsOperationInTransit(string operationId)
        {
            return TransitQueue.ContainsKey(operationId);
        }
    }
}
