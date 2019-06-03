using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class RequestClusterTasksUpsert : BaseRequest<RequestClusterTasksUpsertResponse>
    {
        public List<BaseTask> Tasks { get; set; }

        public override string RequestName => "RequestClusterTasksUpsert";
    }

    public class RequestClusterTasksUpsertResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
