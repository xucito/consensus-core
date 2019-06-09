using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class UpdateData : BaseRequest<UpdateDataResponse>
    {
        public Guid Id { get; set; }
        public string Type { get; set; }
        /// <summary>
        /// This can be the whole object or a partial update
        /// </summary>
        public object Update { get; set; }

        public override string RequestName => "UpdateData";
    }

    public class UpdateDataResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
