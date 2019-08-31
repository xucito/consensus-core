using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class ObjectLock
    {
        public Guid LockId { get; set; }
        public Guid ObjectId { get; set; }
        public string Type { get; set; }
        public DateTime CreatedOn { get; } = DateTime.Now;
        public int LockTimeoutMs { get; set; }
        public bool IsExpired { get { return (DateTime.Now - CreatedOn).TotalMilliseconds > LockTimeoutMs; } }
    }
}
