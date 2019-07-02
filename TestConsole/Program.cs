using ConsensusCore.Domain.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Management;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace TestConsole
{
    class Program
    {
        public static ILogger logger;
        static Random rand = new Random();
        static Thread chaosMonkey;
        static object DataLock = new object();
        static int TestLoops = 0;
        static int RandomFailures = 0;
        static int TotalTimeDown = 0;
        static int AverageTimeForRecoveries = 0;
        static int[] ports = new int[]
        {
            5021,
            5022,
            5023
        };
        /* static string[] Urls = new string[]
         {
             "https://localhost:5021",
             "https://localhost:5022",
             "https://localhost:5023"
         };*/

        static List<string> Urls { get { return ports.Select(p => "https://localhost:" + p).ToList(); } }
        static bool EnableChaosMonkey = true;
        static Dictionary<int, Process> Processes = new Dictionary<int, Process>();

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            ILoggerFactory loggerFactory = new LoggerFactory()
             .AddConsole();
            logger = loggerFactory.CreateLogger<Program>();
            ///var client = new Client("https://localhost:5022");
            //client.AddValue(1).GetAwaiter().GetResult();


            if (EnableChaosMonkey)
            {
                string strCmdText;
                strCmdText = "//k .//ConsensusCore.TestNode//Node_5021.bat";
                foreach(var port in ports)
                {
                    Processes.Add(port, System.Diagnostics.Process.Start("CMD.exe", "/k C:\\Users\\TNguy\\OneDrive\\Documents\\Repositories\\consensus-core\\ConsensusCore.TestNode\\Node_"+port+".bat"));
                }
                /*Processes.Add(Urls[0], System.Diagnostics.Process.Start("CMD.exe", "/k C:\\Users\\TNguy\\OneDrive\\Documents\\Repositories\\consensus-core\\ConsensusCore.TestNode\\Node_5021.bat"));
                Processes.Add(Urls[1], System.Diagnostics.Process.Start("CMD.exe", "/k C:\\Users\\TNguy\\OneDrive\\Documents\\Repositories\\consensus-core\\ConsensusCore.TestNode\\Node_5022.bat"));*/
              //  Processes.Add(Urls[2], System.Diagnostics.Process.Start("CMD.exe", "/k C:\\Users\\TNguy\\OneDrive\\Documents\\Repositories\\consensus-core\\ConsensusCore.TestNode\\Node_5023.bat"));

                chaosMonkey = new Thread(() =>
                {
                    ChaosMonkeyThread(Processes);
                });
                chaosMonkey.Start();
            }
            while (true)
            {
                List<Task> allThreads = new List<Task>();
                int numberOfConcurrentThreads = 10;
                lock (DataLock)
                {
                    for (var i = 0; i < numberOfConcurrentThreads; i++)
                    {
                        allThreads.Add(new Task(async () =>
                        {
                            try
                            {
                                Interlocked.Increment(ref TestLoops);
                                await RunTest();
                            }
                            catch (Exception e)
                            {
                                logger.LogError("Critical error while running test...");
                            }
                        }));
                    }
                    Parallel.ForEach(allThreads, thread =>
                             {
                                 thread.Start();
                             });

                    Task.WhenAll(allThreads).GetAwaiter().GetResult();
                }
            }
            Console.ReadLine();
        }

        public static void PrintStatus()
        {
            if (RandomFailures != 0)
            {
                Console.WriteLine("Number of tests run: " + TestLoops + " | Simulated Failures: " + RandomFailures + " | Average Recovery Time " + (TotalTimeDown / RandomFailures));
            }
        }

        public static async void ChaosMonkeyThread(Dictionary<int, Process> processes)
        {
            var client = new Client();
            while (true)
            {
                PrintStatus();
                Thread.Sleep(rand.Next(0, 10000));
                var numberOfNodeFailures = (processes.Count - 1) / 2;


                List<int> processesToDisable = new List<int>();
                // if the number of 
                if (NumberOfAliveNodes() == ports.Length)
                {
                    int number;
                    for (int i = 0; i < numberOfNodeFailures; i++)
                    {
                        do
                        {
                            number = rand.Next(0, ports.Length);
                        } while (processesToDisable.Contains(number));
                        processesToDisable.Add(number);
                    }
                }
                else
                {
                    logger.LogDebug("Not all threads are ready.");
                }

                if (processesToDisable.Count > 0)
                {
                    var killTime = DateTime.Now;

                    foreach (var number in processesToDisable)
                    {
                        ThreadManager.KillProcessAndChildrens(Processes[ports[number]].Id);
                        /*while(System.Diagnostics.Process.GetProcessById(Processes[Urls[number]].Id) != null)
                        {
                            Thread.Sleep(1000);
                        }*/
                        //ThreadManager.Suspend(Processes[Urls[number]]);
                        logger.LogInformation("Disabling node " + number);

                        Thread.Sleep(100);
                    }

                    Interlocked.Increment(ref RandomFailures);

                    while (NumberOfAliveNodes() < (ports.Length - numberOfNodeFailures))
                    {
                        logger.LogDebug("Cluster has not recovered...");
                    }

                    Interlocked.Add(ref TotalTimeDown, (int)(DateTime.Now - killTime).TotalMilliseconds);

                    logger.LogInformation("Cluster recovery took " + (DateTime.Now - killTime).TotalMilliseconds + "ms");

                    foreach (var number in processesToDisable)
                    {
                        logger.LogInformation("Recovering node " + number);
                        Processes[ports[number]].Start();

                        while ((client.GetNodeStatus(Urls[number]).GetAwaiter().GetResult() == NodeStatus.Green))
                        {
                            logger.LogDebug("Node " + number + " has not recovered. ");
                        }

                        logger.LogInformation("Node " + number + " is recovered.");
                    }
                }

                lock (DataLock)
                {
                    while (!client.AreAllNodesGreen(Urls.ToArray()).GetAwaiter().GetResult())
                    {
                        Thread.Sleep(1000);
                    }

                    if (client.IsClusterDataStoreConsistent(Urls.ToList()).GetAwaiter().GetResult())
                    {

                        Console.WriteLine("CONGRATULATIONS, THE CLUSTER RECOVERED CONSISTENTLY.");
                    }
                    else
                    {

                        Console.WriteLine("ERROR! THERE IS AN ISSUE WITH THE DATA CONSISTENCY AFTER RECOVERY.");
                        Thread.Sleep(10000);
                        while (!client.AreAllNodesGreen(Urls.ToArray()).GetAwaiter().GetResult())
                        {
                            Thread.Sleep(1000);
                        }
                        while (!client.IsClusterDataStoreConsistent(Urls.ToList()).GetAwaiter().GetResult())
                        {
                            Console.WriteLine("After 10 seconds the cluster is still inconsistent.");
                            Thread.Sleep(1000);
                        }
                        Console.WriteLine("CONGRATULATIONS, THE CLUSTER RECOVERED CONSISTENTLY.");
                        /*if (client.IsClusterDataStoreConsistent(Urls.ToList()).GetAwaiter().GetResult())
                        {
                            Console.WriteLine("After 10 seconds the cluster reported as consistent.");
                        }
                        else
                        {
                            Console.WriteLine("After 10 seconds the cluster is still inconsistent.");

                        }*/
                    }

                }
            }
        }

        public static int NumberOfAliveNodes()
        {

            var client = new Client();
            var numberOfAliveNodes = 0;
            foreach (var url in Urls)
            {
                if (client.IsNodeInCluster(url).GetAwaiter().GetResult())
                {
                    numberOfAliveNodes++;
                }
            }
            return numberOfAliveNodes;
        }


        public static string GetRandomUrl()
        {
            return Urls[rand.Next(0, Urls.Count())];
        }

        public static async Task<bool> RunTest()
        {
            var Client = new Client();
            var testedValue = rand.Next(0, 99999);

            var addResult = TrySend(async () => await Client.AddValue(testedValue)).GetAwaiter().GetResult();

            if (addResult != null && (TrySend(async () => await Client.GetValue(addResult.Item2)).GetAwaiter().GetResult().Item2) == testedValue)
            {
                logger.LogDebug("Successfully added the object");
            }
            else
            {
                return false;
            }
            var updatedValue = rand.Next(0, 99999);
            TrySend(async () => await Client.UpdateValue(addResult.Item2, updatedValue)).GetAwaiter().GetResult();
            /* if ((TrySend(async () => await Client.GetValue(getUrl, result))).GetAwaiter().GetResult() == updatedValue)
             {
                 Console.WriteLine("Successfully updated the object");
             }
             else
             {
                 Console.WriteLine("Critical Concurrency Issue detected during update!");
                 Console.WriteLine("Added to " + addUrl);
                 Console.WriteLine("Updated to " + updatedUrl);*/
            foreach (var url in Urls)
            {
                var result = (TrySend(async () => await Client.GetValue(addResult.Item2, url))).GetAwaiter().GetResult();

                if (result == null)
                {
                    logger.LogDebug(url + " did not accept the request.");
                }
                else if (result != null && result.Item2 == updatedValue)
                {
                    logger.LogDebug("Data is consistent on node " + url);
                }
                else
                {
                    logger.LogError("Data is inconsistent on node " + url);
                }
            }

            // }
            return true;
        }


        public static async Task<TResult> TrySend<TResult>(Func<Task<TResult>> action)
        {
            var counters = 0;
            while (counters < 4)
            {
                try
                {
                    var result = await action();
                    if (result == null)
                    {
                        throw new Exception();
                    }
                    return result;
                }
                catch (Exception e)
                {
                    logger.LogDebug("Failed to send... trying again");
                }
                counters++;
            }
            logger.LogWarning("Failed 10 times");
            return default(TResult);
        }
    }

    public static class ThreadManager
    {

        [DllImport("kernel32.dll")]
        static extern IntPtr OpenThread(ThreadAccess dwDesiredAccess, bool bInheritHandle, uint dwThreadId);
        [DllImport("kernel32.dll")]
        static extern uint SuspendThread(IntPtr hThread);
        [DllImport("kernel32.dll")]
        static extern int ResumeThread(IntPtr hThread);

        [Flags]
        public enum ThreadAccess : int
        {
            TERMINATE = (0x0001),
            SUSPEND_RESUME = (0x0002),
            GET_CONTEXT = (0x0008),
            SET_CONTEXT = (0x0010),
            SET_INFORMATION = (0x0020),
            QUERY_INFORMATION = (0x0040),
            SET_THREAD_TOKEN = (0x0080),
            IMPERSONATE = (0x0100),
            DIRECT_IMPERSONATION = (0x0200)
        }

        public static void Suspend(this Process process)
        {
            foreach (ProcessThread thread in process.Threads)
            {
                var pOpenThread = OpenThread(ThreadAccess.SUSPEND_RESUME, false, (uint)thread.Id);
                if (pOpenThread == IntPtr.Zero)
                {
                    break;
                }
                SuspendThread(pOpenThread);
            }
        }
        public static void Resume(this Process process)
        {
            foreach (ProcessThread thread in process.Threads)
            {
                var pOpenThread = OpenThread(ThreadAccess.SUSPEND_RESUME, false, (uint)thread.Id);
                if (pOpenThread == IntPtr.Zero)
                {
                    break;
                }
                ResumeThread(pOpenThread);
            }
        }




        public static void KillProcessAndChildrens(int pid)
        {
            ManagementObjectSearcher processSearcher = new ManagementObjectSearcher
              ("Select * From Win32_Process Where ParentProcessID=" + pid);
            ManagementObjectCollection processCollection = processSearcher.Get();

            try
            {
                Process proc = Process.GetProcessById(pid);
                if (!proc.HasExited) proc.Kill();
                proc.WaitForExit();
            }
            catch (ArgumentException)
            {
                // Process already exited.
            }

            if (processCollection != null)
            {
                foreach (ManagementObject mo in processCollection)
                {
                    KillProcessAndChildrens(Convert.ToInt32(mo["ProcessID"])); //kill child processes(also kills childrens of childrens etc.)
                }
            }
        }
    }

    public class Client
    {
        static string[] Urls = new string[]
         {
                     "https://localhost:5021",
                     "https://localhost:5022",
                     "https://localhost:5023"
         };
        static Random rand = new Random();

        public Client()
        {
        }

        public async Task<Tuple<string, Guid>> AddValue(int value, string url = null)
        {
            try
            {
                if (url == null)
                {
                    url = Urls[rand.Next(0, Urls.Length)];
                }

                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var result = await client.PostAsJsonAsync("api/values", value);
                    var text = (await result.Content.ReadAsStringAsync()).Replace("\"", "");

                    return new Tuple<string, Guid>(url, new Guid(text));
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public async Task<Tuple<string, int>> GetValue(Guid id, string url = null)
        {
            try
            {
                if (url == null)
                {
                    url = Urls[rand.Next(0, Urls.Length)];
                }

                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var result = await client.GetAsync("api/values/" + id);

                    var data = JObject.Parse(await result.Content.ReadAsStringAsync());

                    return new Tuple<string, int>(url, data["data"]["data"].Value<int>());
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public async Task<Tuple<string, bool>> UpdateValue(Guid id, int newValue, string url = null)
        {
            try
            {
                if (url == null)
                {
                    url = Urls[rand.Next(0, Urls.Length)];
                }

                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var result = await client.PutAsJsonAsync("api/values/" + id, newValue);
                    return new Tuple<string, bool>(url, true);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public async Task<bool> IsNodeInCluster(string url)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var result = await client.GetAsync("api/node");
                    var jsonResult = JObject.Parse(await result.Content.ReadAsStringAsync());
                    return jsonResult.Value<bool>("inCluster");
                }
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public async Task<bool> AreAllNodesGreen(string[] urls)
        {
            try
            {
                foreach (var url in urls)
                {
                    using (var client = new HttpClient())
                    {
                        client.BaseAddress = new Uri(url);
                        var result = await client.GetAsync("api/node");
                        var jsonResult = JObject.Parse(await result.Content.ReadAsStringAsync());
                        if (jsonResult.Value<int>("status") != (int)NodeStatus.Green)
                        {
                            return false;
                        }
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public async Task<NodeStatus> GetNodeStatus(string url)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);
                    var result = await client.GetAsync("api/node");
                    var jsonResult = JObject.Parse(await result.Content.ReadAsStringAsync());
                    return (NodeStatus)jsonResult.Value<int>("status");
                }
            }
            catch (Exception e)
            {
                return NodeStatus.Unknown;
            }
        }

        public async Task<bool> IsClusterDataStoreConsistent(List<string> urls)
        {
            try
            {
                List<JArray> allResults = new List<JArray>();
                foreach (var url in urls)
                {
                    using (var client = new HttpClient())
                    {
                        client.BaseAddress = new Uri(url);
                        var result = await client.GetAsync("api/values/Test");
                        allResults.Add(JArray.Parse(await result.Content.ReadAsStringAsync()));
                    }
                }

                for (var i = 1; i < urls.Count; i++)
                {
                    if (!JToken.DeepEquals(allResults[i], allResults[i - 1]))
                    {
                        return false;
                    }
                }
            }
            catch (Exception e)
            {
                return false;
            }
            return true;
        }
    }
}
