using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.BaseClasses
{
    public class Lock
    {
        public Guid LockId { get; set; }
        public string Name { get; set; }
        public DateTime CreatedOn { get; set; }
        public int LockTimeoutMs { get; set; }
        public bool IsExpired() { return (DateTime.Now - CreatedOn).TotalMilliseconds > LockTimeoutMs; }
    }
}
