using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Domain.Exceptions
{
    public class DataShardingException : Exception
    {

        public DataShardingException()
        {
        }

        public DataShardingException(string message)
            : base(message)
        {
        }

        public DataShardingException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
