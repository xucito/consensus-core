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
    public partial class NodeInMemoryRepository<Z> 
        where Z : BaseState, new()
    {
        public ConcurrentBag<DataReversionRecord> DataReversionRecords { get; set; } = new ConcurrentBag<DataReversionRecord>();
        public ConcurrentDictionary<string, ShardWriteOperation> ShardWriteOperations { get; set; } = new ConcurrentDictionary<string, ShardWriteOperation>();
        public ConcurrentBag<ObjectDeletionMarker> ObjectDeletionMarker { get; set; } = new ConcurrentBag<ObjectDeletionMarker>();
        public List<ShardWriteOperation> OperationQueue { get; set; } = new List<ShardWriteOperation>();
        public Dictionary<string, ShardWriteOperation> TransitQueue { get; set; } = new Dictionary<string, ShardWriteOperation>();
        public Dictionary<Guid, ShardMetadata> ShardMetadata { get; set; } = new Dictionary<Guid, ShardMetadata>();
        public object queueLock = new object();


        public NodeInMemoryRepository()
        {
        }
    }
}
