using ConsensusCore.Domain.Exceptions;
using ConsensusCore.Domain.Interfaces;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading;

namespace ConsensusCore.Domain.BaseClasses
{
    public class LocalShardMetaData
    {
        public Guid ShardId { get; set; }
        public string Type { get; set; }
        public int SyncPos = 0;
    }
}
