﻿using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Models
{
    public class NodeInformation
    {
        public string Name { get; set; }
        public string TransportAddress { get; set; }
        public Guid Id { get; set; }
        public bool IsContactable { get; set; }

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
