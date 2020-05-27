using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.Domain.Interfaces;
using ConsensusCore.Domain.Models;
using ConsensusCore.Node;
using ConsensusCore.Node.Communication.Controllers;
using ConsensusCore.Node.Repositories;
using ConsensusCore.Node.Services;
using ConsensusCore.Node.Services.Data;
using ConsensusCore.Node.Services.Raft;
using ConsensusCore.Node.Services.Tasks;
using ConsensusCore.Node.Utility;
using ConsensusCore.TestNode.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Swashbuckle.AspNetCore.Swagger;

namespace ConsensusCore.TestNode
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }
        
        //Used for testing
        public static bool Killed = false;

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton<IDataRouter, TestDataRouter>();
            services.AddConsensusCore<TestState, NodeInMemoryRepository<TestState>, NodeInMemoryRepository<TestState>, NodeInMemoryRepository<TestState>>(s => new NodeInMemoryRepository<TestState>(), s => new NodeInMemoryRepository<TestState>(), s => new NodeInMemoryRepository<TestState>(), Configuration.GetSection("Node"), Configuration.GetSection("Cluster"));
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new Info { Title = "My API", Version = "v1" });
            });

            services.AddMvc(options =>
            {
                options.Conventions.Add(new RouteTokenTransformerConvention(
                new SlugifyParameterTransformer()));
            })
            .AddJsonOptions(options => {
                options.SerializerSettings.NullValueHandling = NullValueHandling.Ignore;
            })
            .SetCompatibilityVersion(CompatibilityVersion.Version_2_2);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env,
            IBaseRepository<TestState> repository,
            IRaftService raftService,
            IDataService dataService,
            ITaskService taskService,
            ILogger<Startup> logger,
            IClusterRequestHandler clusterRequestHandler)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            //  node.MetricGenerated += metricGenerated;

            clusterRequestHandler.MetricGenerated += metricGenerated;

            app.Use(async (context, next) =>
            {
                if (context.Request.Path == "/api/kill" && context.Request.Method == "POST")
                {
                    Killed = true;
                    logger.LogInformation("Killing node");
                    raftService.SetNodeRole(Domain.Enums.NodeState.Disabled);
                }

                if (context.Request.Path == "/api/revive" && context.Request.Method == "POST")
                {
                    Killed = false;
                    logger.LogInformation("Restoring node");
                    raftService.SetNodeRole(Domain.Enums.NodeState.Follower);
                }

                if (context.Request.Path == "/api/delete" && context.Request.Method == "POST")
                {
                    System.Environment.Exit(0);
                }

                if (Killed == false)
                {
                    await next();
                }
                else
                {
                    context.Abort();
                }
            });

            app.UseSwagger();

            // Enable middleware to serve swagger-ui (HTML, JS, CSS, etc.), 
            // specifying the Swagger JSON endpoint.
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "My API V1");
            });

            app.UseHttpsRedirection();
            app.UseMvc();
        }

        static void metricGenerated(object sender, Metric e)
        {
          //  Console.WriteLine(JsonConvert.SerializeObject(e, Formatting.Indented));
        }
    }
}
