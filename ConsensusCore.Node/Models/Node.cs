using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Models
{
    public class NodeInformation
    {
        public string Name { get; set; }
        public string TransportAddress { get; set; }

        public override bool Equals(object obj)
        {
            if ((obj == null) || !this.GetType().Equals(obj.GetType()))
            {
                return false;
            }
            else
            {
                NodeInformation p = (NodeInformation)obj;
                return (Name == p.Name) && (TransportAddress == TransportAddress
                    );
            }
        }
    }
}
