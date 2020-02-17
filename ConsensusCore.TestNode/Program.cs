using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ConsensusCore.TestNode.Models;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NetEscapades.Extensions.Logging.RollingFile;

namespace ConsensusCore.TestNode
{
    public class Program
    {
        static IConfigurationRoot config;
        public static void Main(string[] args)
        {
            try
            {
                config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false)
                .AddEnvironmentVariables()
                .Build();

                CreateWebHostBuilder(args).Build().Run();
            }
            catch (Exception e)
            {
                Console.WriteLine("Encountered Global Critical error "  +  e.Message + Environment.NewLine + e.StackTrace);
            }
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .ConfigureLogging(builder => builder.AddFile(options =>
                {
                    options.FileName = "diagnostics-" + config.GetValue<string>("Node:Name") + "_"; // The log file prefixes
                    options.LogDirectory = "LogFiles"; // The directory to write the logs
                    options.FileSizeLimit = 100 * 1024 * 1024; // The maximum log file size (20MB here)
                    options.Extension = "txt"; // The log file extension
                    options.Periodicity = PeriodicityOptions.Hourly; // Roll log files hourly instead of daily.
                }))
                .UseStartup<Startup>();
    }
}
