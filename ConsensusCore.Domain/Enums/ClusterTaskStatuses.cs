﻿using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Enums
{
    public enum ClusterTaskStatuses
    {
        Created,
        Successful,
        Cancelled,
        Paused,
        Error,
        InProgress
    }
}
