using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs.Raft
{
    /*public class InstallSnapshot<Z> :
        BaseRequest<InstallSnapshotResponse>
        where Z : BaseState
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int LastIncludedIndex { get; set; }
        public int LastIncludedTerm { get; set; }
        /// <summary>
        /// The snapshot to be sent from the offset, the snapshot is always sent in chunks
        /// </summary>
        public Z Snapshot { get; set; }

        public override string RequestName => "InstallSnapshot";
    }*/

    public class InstallSnapshot :  BaseRequest<InstallSnapshotResponse>
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int LastIncludedIndex { get; set; }
        public int LastIncludedTerm { get; set; }
        /// <summary>
        /// The snapshot to be sent from the offset, the snapshot is always sent in chunks
        /// </summary>
        public object Snapshot { get; set; }

        public override string RequestName => "InstallSnapshot";
    }

    public class InstallSnapshotResponse : BaseResponse
    {
        public int Term { get; set; }
    }
}
