using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.RPCs;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ConcensusCore.Node.Tests.Cast
{
    public class Cast_Tests
    {
        [Fact]
        public void TestCast()
        {
            IClusterRequest<object> test = (IClusterRequest<object>)new AssignNewShard();
        }

        [Fact]
        public void TestCast2()
        {
            IClusterRequest<object> test = (IClusterRequest<object>)new ExecuteCommands();
        }


        [Fact]
        public void TestCast3()
        {
            IClusterRequest<object> test = (IClusterRequest<object>)new AppendEntry();
        }
    }
}
