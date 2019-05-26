using ConsensusCore.Node.Services;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.BaseClasses
{
    public abstract class BaseRepository
    {
        public abstract void SaveNodeData();
        public abstract NodeStorage LoadNodeData();
    }
}
