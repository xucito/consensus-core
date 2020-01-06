using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.RPCs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Services
{
    public class WriteCache
    {
        public IOperationCacheRepository _operationCacheRepository;
        public ILogger<WriteCache> _logger;
        public int OperationsInQueue { get { return _operationCacheRepository.CountOperationsInQueue(); } }
        public int OperationsInTransit { get { return _operationCacheRepository.CountOperationsInTransit(); } }
        public object queueLock = new object();
        ConcurrentQueue<ShardWriteOperation> operations = new ConcurrentQueue<ShardWriteOperation>();

        public WriteCache(IOperationCacheRepository transactionCacheRepository, ILogger<WriteCache> logger)
        {
            _operationCacheRepository = transactionCacheRepository;
            _logger = logger;

            if (_operationCacheRepository.CountOperationsInTransit() > 0)
            {
                _logger.LogError("There are transactions in transit that have not been applied, retrospective application required...");

                //TODO for each transaction in transit, check the queue to apply
            }
        }

        public bool EnqueueOperation(ShardWriteOperation transaction)
        {
            return _operationCacheRepository.EnqueueOperation(transaction);
        }

        public ShardWriteOperation DequeueOperation()
        {
            lock (queueLock)
            {
                var operation = _operationCacheRepository.GetNextOperation();
                if (operation != null)
                {
                    _operationCacheRepository.AddOperationToTransit(operation);
                    _operationCacheRepository.DeleteOperationFromQueue(operation);
                }
                return operation;
            }
        }

        public bool CompleteOperation(string transactionId)
        {
            return _operationCacheRepository.DeleteOperationFromTransit(transactionId);
        }

        public bool IsOperationComplete(string transactionId)
        {
            return (!_operationCacheRepository.IsOperationInTransit(transactionId) && !_operationCacheRepository.IsOperationInQueue(transactionId));
        }
    }
}
