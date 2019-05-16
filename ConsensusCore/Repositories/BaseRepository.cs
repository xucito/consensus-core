using ConsensusCore.BaseClasses;
using ConsensusCore.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsensusCore.Repositories
{
    public interface INodeRepository<Command> where Command: BaseCommand
    {
         NodeInfo<Command> LoadConfiguration();
    }
}
