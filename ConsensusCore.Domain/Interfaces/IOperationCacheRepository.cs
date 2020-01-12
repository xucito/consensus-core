using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.RPCs;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.Interfaces
{
    public interface IOperationCacheRepository
    {
        /// <summary>
        /// Add a transaction to the queue to be applied to the database
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        Task<bool> EnqueueOperationAsync(ShardWriteOperation data);
        /// <summary>
        /// Gets Next Operation
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        Task<ShardWriteOperation> GetNextOperationAsync();
        /// <summary>
        /// Delete Operation from Queue
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        Task<bool> DeleteOperationFromQueueAsync(ShardWriteOperation operation);
        /// <summary>
        /// Add operation to transit queue
        /// </summary>
        /// <param name="operation"></param>
        /// <returns></returns>
        Task<bool> AddOperationToTransitAsync(ShardWriteOperation operation);
        /// <summary>
        /// Removes the transaction from the queue and moves to transit
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        Task<bool> DeleteOperationFromTransitAsync(string operationId);
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
        /// <summary>
        /// Load the entire Operation Queue
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<ShardWriteOperation>> GetOperationQueueAsync();
        /// <summary>
        /// Load the entire transit queue
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<ShardWriteOperation>> GetTransitQueueAsync();
    }
}
