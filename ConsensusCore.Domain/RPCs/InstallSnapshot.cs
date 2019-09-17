using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.RPCs
{
    public class InstallSnapshot : BaseRequest<InstallSnapshotResponse>
    {
        public int Term { get; set; }
        public Guid LeaderId { get; set; }
        public int LastIncludedIndex { get; set; }
        public int LastIncludedTermOffset { get; set; }
        /// <summary>
        /// The snapshot to be sent from the offset, the snapshot is always sent in chunks
        /// </summary>
        public byte[] Data { get; set; }
        public bool Done { get; set; }

        public override string RequestName => "InstallSnapshot";
    }

    public class InstallSnapshotResponse : BaseResponse
    {
        public int Term { get; set; }
    }
}
