using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ConsensusCore.Interfaces;
using ConsensusCore.Options;
using ConsensusCore.Repositories;
using ConsensusCore.Samples.Calculator;
using ConsensusCore.Services;
using ConsensusCore.Utility;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Swashbuckle.AspNetCore.Swagger;

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

            services.AddSingleton<INodeRepository<Calculate>, NodeInMemoryRepository<Calculate>>();
            services.AddSingleton<INodeManager<Calculate, CalculatorState, StateMachine<Calculate, CalculatorState>, NodeInMemoryRepository<Calculate>>, NodeManager<Calculate, CalculatorState, StateMachine<Calculate, CalculatorState>, NodeInMemoryRepository<Calculate>>>();

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
                .ConfigureApplicationPartManager(apm =>
                    apm.ApplicationParts.Add(new NodeControllerApplicationPart(new Type[] {
                       typeof( Calculate),
                        typeof(CalculatorState),
                        typeof(NodeInMemoryRepository<Calculate>)
                    })));
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new Info { Title = "My API", Version = "v1" });
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, INodeManager<Calculate, CalculatorState, StateMachine<Calculate, CalculatorState>, NodeInMemoryRepository<Calculate>> nodeManager)
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

            app.UseSwagger();

            // Enable middleware to serve swagger-ui (HTML, JS, CSS, etc.), 
            // specifying the Swagger JSON endpoint.
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "My API V1");
            });

            app.UseMvc();
        }
    }
}
