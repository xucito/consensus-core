using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.RPCs;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IOperationCacheRepository
    {
        /// <summary>
        /// Add a transaction to the queue to be applied to the database
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        bool EnqueueOperation(ShardWriteOperation data);
        /// <summary>
        /// Gets Next Operation
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        ShardWriteOperation GetNextOperation();
        /// <summary>
        /// Delete Operation from Queue
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        bool DeleteOperationFromQueue(ShardWriteOperation operation);
        /// <summary>
        /// Add operation to transit queue
        /// </summary>
        /// <param name="operation"></param>
        /// <returns></returns>
        bool AddOperationToTransit(ShardWriteOperation operation);
        /// <summary>
        /// Removes the transaction from the queue and moves to transit
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        bool DeleteOperationFromTransit(string operationId);
        /// <summary>
        /// Removes the transaction from the queue and moves to transit
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        int CountOperationsInQueue();
        /// <summary>
        /// Removes the transaction from the queue and moves to transit
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        int CountOperationsInTransit();
        /// <summary>
        /// Is the operation still in transit
        /// </summary>
        /// <param name="operationId"></param>
        /// <returns></returns>
        bool IsOperationInTransit(string operationId);
        /// <summary>
        /// Is the operation still in transit
        /// </summary>
        /// <param name="operationId"></param>
        /// <returns></returns>
        bool IsOperationInQueue(string operationId);
    }
}
