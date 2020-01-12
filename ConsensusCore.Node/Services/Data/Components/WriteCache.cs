using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs;
using ConsensusCore.Domain.Utility;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Node.Services.Data.Components
{
    public class WriteCache
    {
        public IOperationCacheRepository _operationCacheRepository;
        public ILogger<WriteCache> _logger;
        public int OperationsInQueue { get { return _operationCacheRepository.CountOperationsInQueue(); } }
        public int OperationsInTransit { get { return _operationCacheRepository.CountOperationsInTransit(); } }
        public object queueLock = new object();
        ConcurrentQueue<ShardWriteOperation> operations = new ConcurrentQueue<ShardWriteOperation>();
        //In-memory queue
        public List<ShardWriteOperation> OperationQueue { get; set; } = new List<ShardWriteOperation>();
        public Dictionary<string, ShardWriteOperation> TransitQueue { get; set; } = new Dictionary<string, ShardWriteOperation>();
        private readonly bool _persistToDisk;

        public WriteCache(IOperationCacheRepository transactionCacheRepository, ILogger<WriteCache> logger, bool persistToDisk = true)
        {
            _operationCacheRepository = transactionCacheRepository;
            _logger = logger;
            _persistToDisk = persistToDisk;

            //Load persistent queue into memory
            if (_persistToDisk)
            {
                _logger.LogDebug("Loading transactions to queue...");
                OperationQueue = _operationCacheRepository.GetOperationQueueAsync().GetAwaiter().GetResult().ToList();
                TransitQueue = _operationCacheRepository.GetTransitQueueAsync().GetAwaiter().GetResult().ToList().ToDictionary(swo => swo.Id, swo => swo);
            }
            else
            {
                _logger.LogWarning("Queue has been set to transient mode. Queue data will not be persisted to disk");
            }
        }

        public async Task<bool> EnqueueOperationAsync(ShardWriteOperation transaction)
        {
            lock (queueLock)
            {
                OperationQueue.Add(transaction);
            }
            if (_persistToDisk)
                return await _operationCacheRepository.EnqueueOperationAsync(transaction);
            return true;
        }

        public async Task<ShardWriteOperation> DequeueOperation()
        {
            lock (queueLock)
            {
                var operationQueueResult = OperationQueue.Take(1);
                if (operationQueueResult.Count() > 0)
                {
                    var operation = operationQueueResult.First();
                    TransitQueue.TryAdd(operation.Id, SystemExtension.Clone(operation));
                    OperationQueue.Remove(operation);
                    if (_persistToDisk)
                    {
                        _operationCacheRepository.AddOperationToTransitAsync(operation).GetAwaiter().GetResult();
                        _operationCacheRepository.DeleteOperationFromQueueAsync(operation).GetAwaiter().GetResult();
                    }
                    return operation;
                }
                return null;
            }
        }

        public async Task<bool> CompleteOperation(string transactionId)
        {
            lock (queueLock)
            {
                TransitQueue.Remove(transactionId);
            }
            if (_persistToDisk)
            {
                return await _operationCacheRepository.DeleteOperationFromTransitAsync(transactionId);
            }
            return true;
        }

        public bool IsOperationComplete(string transactionId)
        {
            lock (queueLock)
            {
                var isOperationInQueue = OperationQueue.Find(oq => oq.Id == transactionId) != null;
                return (!TransitQueue.ContainsKey(transactionId) && !isOperationInQueue);
            }
        }
    }
}
