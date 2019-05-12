using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using ConsensusCore.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ConsensusCore
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.Configure<ClusterOptions>(Configuration.GetSection("Cluster"));
            services.Configure<NodeOptions>(Configuration.GetSection("Node"));

            services.AddSingleton<INodeRepository, NodeInMemoryRepository>();
            services.AddSingleton<INodeManager, NodeManager<NodeInMemoryRepository>>();

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, INodeManager nodeManager)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseHsts();

                app.UseHttpsRedirection();
            }

            /*app.Use(async (context, next) =>
            {
                var nodeId = context.Request.Headers["nodeId"].FirstOrDefault();

                if(nodeId != null)
                {
                    manager.RegisterNodeRequest(nodeId);
                }

                await next.Invoke();
            });*/

            app.UseMvc();
        }
    }
}
