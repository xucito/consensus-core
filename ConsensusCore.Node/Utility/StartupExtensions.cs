using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Controllers;
using ConsensusCore.Node.Interfaces;
using ConsensusCore.Node.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusCore.Node.Utility
{
    public static class StartupExtensions
    {
        public static void AddConsensusCore<State, Repository>(this IServiceCollection services)
            where State : BaseState, new()
            where Repository : BaseRepository
        {
            services.AddSingleton<Repository>();
            services.AddSingleton<NodeStorage>();
            services.AddSingleton<StateMachine<State>>();
            services.AddSingleton<IConsensusCoreNode<State, Repository>, ConsensusCoreNode<State, Repository>>();
            services.AddTransient<NodeController<State, Repository>>();

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                        typeof(State),
                        typeof(Repository)
                    })));

        }
    }
}
