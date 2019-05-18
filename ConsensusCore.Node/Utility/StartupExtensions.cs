using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Controllers;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Utility
{
    public static class StartupExtensions
    {
        public static void AddConsensusCore<Command, State, Repository>(this IServiceCollection services)
            where Command : BaseCommand
            where State : BaseState<Command>, new()
            where Repository : BaseRepository<Command>
        {
            services.AddSingleton<Repository>();
            services.AddSingleton<NodeStorage<Command, Repository>>();
            services.AddSingleton<ConsensusCoreNode<Command, State, Repository>>();
            services.AddTransient<NodeController<Command, State, Repository>>();

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                       typeof(Command),
                        typeof(State),
                        typeof(Repository)
                    })));
        }
    }
}
