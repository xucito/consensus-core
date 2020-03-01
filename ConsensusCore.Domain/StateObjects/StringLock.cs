using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.StateObjects
{
    public class Lock
    {
        public Guid LockId { get; set; }
        /// <summary>
        /// The name of the string lock to apply, this is the unique identifier of the lock
        /// </summary>
        public string Name { get; set; }
        public int LockTimeoutMs { get; set; }
        public DateTime CreatedOn { get; set; }
        public bool IsExpired { get { return (DateTime.Now - CreatedOn).TotalMilliseconds > LockTimeoutMs; } }
    }
}
