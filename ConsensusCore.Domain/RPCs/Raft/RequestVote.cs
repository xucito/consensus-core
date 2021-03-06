﻿using ConsensusCore.Domain.BaseClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Domain.RPCs.Raft
{
    public class RequestVote : BaseRequest<RequestVoteResponse>
    {
        public int Term { get; set; }
        public Guid CandidateId { get; set; }
        public int LastLogIndex { get; set; }
        public int LastLogTerm { get; set; }

        public override string RequestName => "RequestVote";
    }

    public class RequestVoteResponse: BaseResponse
    {
        public Guid NodeId { get; set; }
    }
}
