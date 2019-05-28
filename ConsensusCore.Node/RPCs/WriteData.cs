using ConsensusCore.Node.BaseClasses;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.RPCs
{
    public class WriteData : BaseRequest<WriteDataResponse>
    {
        public string Type { get; set; }
        public object Data { get; set; }
        public int? Version { get; set; }
        // Wait for at least two copies of the data to be present
        public bool WaitForSafeWrite { get; set; }
        public override string RequestName => "WriteDataShard";
        /// <summary>
        /// Only used to optimize after rerouting requests
        /// </summary>
        internal Guid? AssignedShard { get; set; }
    }

    public class WriteDataResponse
    {
        public bool IsSuccessful { get; set; }
    }
}
