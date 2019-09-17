using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Services;
using ConsensusCore.Node.Connectors;
using ConsensusCore.Node.Controllers;
using ConsensusCore.Node.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using System.Text;
using Microsoft.Extensions.DependencyInjection;

namespace ConsensusCore.Node.Utility
{
    public static class StartupExtensions
    {
        /*public static void AddConsensusCore<State, Repository>(this IServiceCollection services)
            where State : BaseState, new()
            where Repository : class, IBaseRepository
        {
            services.AddSingleton<IBaseRepository, Repository>();
            //services.AddSingleton<NodeStorage>();
            services.AddSingleton<StateMachine<State>>();
            services.AddSingleton<IConsensusCoreNode<State, Repository>, ConsensusCoreNode<State, Repository>>();
            services.AddTransient<NodeController<State, Repository>>();

            services.AddMvcCore().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                        typeof(State),
                        typeof(Repository)
                    })));

        }*/

        public static void AddConsensusCore<State, Repository>(this IServiceCollection services, Func<IServiceProvider, Repository> implementationFactory,
            Action<NodeOptions> nodeOptions,
            Action<ClusterOptions> clusterOptions)
                where State : BaseState, new()
                where Repository : class, IBaseRepository
        {
            services.AddSingleton<IBaseRepository, Repository>(implementationFactory);
            // services.AddSingleton<NodeStorage>();
            services.AddSingleton<IStateMachine<State>, StateMachine<State>>();
            services.AddSingleton<IConsensusCoreNode<State, IBaseRepository>, ConsensusCoreNode<State, IBaseRepository>>();
            services.AddTransient<NodeController<State, IBaseRepository>>();
            services.Configure(nodeOptions);
            services.Configure(clusterOptions);
            services.AddSingleton<ClusterConnector>();

            services.AddMvcCore().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                        typeof(State),
                        typeof(Repository)
                    })));

        }

        public static void AddConsensusCore<State, Repository>(this IServiceCollection services, Func<IServiceProvider, Repository> implementationFactory,
     IConfigurationSection nodeOptions,
    IConfigurationSection clusterOptions)
        where State : BaseState, new()
        where Repository : class, IBaseRepository
        {
            services.AddSingleton<IBaseRepository, Repository>(implementationFactory);
            // services.AddSingleton<NodeStorage>();
            services.AddSingleton<IStateMachine<State>, StateMachine<State>>();
            services.AddSingleton<IConsensusCoreNode<State, IBaseRepository>, ConsensusCoreNode<State, IBaseRepository>>();
            services.AddTransient<NodeController<State, IBaseRepository>>();
            services.Configure<NodeOptions>(nodeOptions);
            services.Configure<ClusterOptions>(clusterOptions);
            services.AddSingleton<ClusterConnector>();
            services.AddSingleton<NodeStorage>();
            services.AddSingleton<ShardManager<State, IBaseRepository>>();

            services.AddMvcCore().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                        typeof(State),
                        typeof(Repository)
                    })));

        }
    }
}
