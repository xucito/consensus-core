using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.ViewModels
{
    public class PostLockRequest
    {
        public string Type { get; set; }
        public Guid Id { get; set; }
        public int LockTimeoutMs { get; set; }
    }
}
