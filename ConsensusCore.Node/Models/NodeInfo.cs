﻿using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Models
{
    public class NodeInfo
    {
        public Guid Id { get; set; }
        public int ShardCount { get; set; }
    }
}
