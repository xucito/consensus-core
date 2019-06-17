using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace TestConsole
{
    class Program
    {
        static Random rand = new Random();
        static string[] Urls = new string[]
        {
            "https://localhost:5021",
            "https://localhost:5022",
            "https://localhost:5023"
        };

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            ///var client = new Client("https://localhost:5022");
            //client.AddValue(1).GetAwaiter().GetResult();

            while (true)
            {
                List<Task> allThreads = new List<Task>();
                allThreads.Add(new Task(async () =>
                {
                    await RunTest();
                }));

                Parallel.ForEach(allThreads, thread =>
                         {
                             thread.Start();
                         });

                Task.WhenAll(allThreads).GetAwaiter().GetResult();
            }
            Console.ReadLine();
        }

        public static string GetRandomUrl()
        {
            return Urls[rand.Next(0, Urls.Length)];
        }

        public static async Task<bool> RunTest()
        {
            var Client = new Client();
            var testedValue = rand.Next(0, 99999);

            var addResult = TrySend(async () => await Client.AddValue(testedValue)).GetAwaiter().GetResult();

            if ((TrySend(async () => await Client.GetValue(addResult.Item2)).GetAwaiter().GetResult().Item2) == testedValue)
            {
                Console.WriteLine("Successfully added the object");
            }
            else
            {
                Console.WriteLine("Critical Concurrency Issue detected during creation!");
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

                if(result == null)
                {
                    Console.WriteLine(url + " did not accept the request.");
                }
                else if (result != null && result.Item2 == updatedValue)
                {
                    Console.WriteLine("Data is consistent on node " + url);
                }
                else
                {
                    Console.WriteLine("Data is inconsistent on node " + url);
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
                    return result;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to send... trying again");
                }
                counters++;
            }
            Console.WriteLine("Failed 10 times");
            return default(TResult);
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
    }
}
