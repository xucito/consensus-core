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
using ConsensusCore.Node.Communication.Clients;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.Communication.Controllers;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.Node.Services.Tasks;

namespace ConsensusCore.Node.Utility
{
    public static class StartupExtensions
    {

        public static void AddConsensusCore<State, Repository, ShardRepository, operationCache>(this IServiceCollection services, Func<IServiceProvider, Repository> implementationFactory,
            Func<IServiceProvider, ShardRepository> shardRepositoryImplementationFactory,
            Action<NodeOptions> nodeOptions,
            Action<ClusterOptions> clusterOptions
            )
                where State : BaseState, new()
                where Repository : class, IBaseRepository<State>
        where ShardRepository : class, IShardRepository
            where operationCache : class, IOperationCacheRepository
        {
            services.AddSingleton<IOperationCacheRepository, operationCache>();
            services.AddSingleton<IBaseRepository<State>, Repository>(implementationFactory);
            // services.AddSingleton<NodeStorage>();
            services.AddSingleton<IStateMachine<State>, StateMachine<State>>();
            services.AddSingleton<INodeStorage<State>, NodeStorage<State>>();
            services.AddSingleton<NodeStateService>();
            services.AddSingleton<IClusterConnectionPool, ClusterConnectionPool<State>>();
            services.AddSingleton<ClusterClient>();
            services.AddSingleton<IRaftService, RaftService<State>>();
            services.AddTransient<NodeController<State>>();
            services.Configure(nodeOptions);
            services.Configure(clusterOptions);
            services.AddSingleton<ClusterClient>();
            services.AddSingleton<WriteCache>();
            services.AddTransient<IClusterRequestHandler, ClusterRequestHandler<State>>();
            services.AddSingleton<IDataService, DataService<State>>();
            services.AddSingleton<ITaskService, TaskService<State>>();
            services.AddMvcCore().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                        typeof(State),
                        typeof(Repository),
                        typeof(ShardRepository)
                    })));
        }

        public static void AddConsensusCore<State, Repository, ShardRepository, operationCache>(this IServiceCollection services,
            Func<IServiceProvider, Repository> implementationFactory,
            Func<IServiceProvider, ShardRepository> shardRepositoryImplementationFactory,
            IConfigurationSection nodeOptions,
            IConfigurationSection clusterOptions)
                where State : BaseState, new()
                where Repository : class, IBaseRepository<State>
                where ShardRepository : class, IShardRepository
            where operationCache : class, IOperationCacheRepository
        {
            services.AddSingleton<IOperationCacheRepository, operationCache>();
            services.AddSingleton<WriteCache>();
            services.AddSingleton<IBaseRepository<State>, Repository>(implementationFactory);
            services.AddSingleton<IShardRepository, ShardRepository>(shardRepositoryImplementationFactory);
            // services.AddSingleton<NodeStorage
            services.AddSingleton<IStateMachine<State>, StateMachine<State>>();
            services.AddSingleton<IClusterConnectionPool, ClusterConnectionPool<State>>();
            services.AddSingleton<NodeStateService>();
            services.AddSingleton<ClusterClient>();
            services.AddSingleton<INodeStorage<State>, NodeStorage<State>>();
            services.AddSingleton<IRaftService, RaftService<State>>();
            services.AddTransient<NodeController<State>>();
            services.Configure<NodeOptions>(nodeOptions);
            services.Configure<ClusterOptions>(clusterOptions);
            services.AddSingleton<ClusterClient>();
            services.AddSingleton<NodeStorage<State>>();
            services.AddSingleton<ITaskService, TaskService<State>>();
            services.AddTransient<IClusterRequestHandler, ClusterRequestHandler<State>>();
            services.AddSingleton<IDataService, DataService<State>>();

            services.AddMvcCore().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                        typeof(State)
                    })));

        }
    }
}
