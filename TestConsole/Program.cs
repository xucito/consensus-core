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



        public static async Task<bool> RunTest()
        {
            var Client = new Client();
            var testedValue = rand.Next(0, 99999);
            var result =  TrySend(async () => await Client.AddValue(testedValue)).GetAwaiter().GetResult();
            if ((TrySend(async () => await Client.GetValue(result)).GetAwaiter().GetResult()) == testedValue)
            {
                Console.WriteLine("Successfully added the object");
            }

            var updatedValue = rand.Next(0, 99999);

            TrySend(async () => await Client.UpdateValue(result, updatedValue)).GetAwaiter().GetResult();
            if ((TrySend(async () => await Client.GetValue(result))).GetAwaiter().GetResult() == updatedValue)
            {
                Console.WriteLine("Successfully updated the object");
            }
            return true;
        }

        public static async Task<TResult> TrySend<TResult>(Func<Task<TResult>> action)
        {
            var counters = 0;
            while (counters < 10)
            {
                try
                {
                    var result = await action();
                    return result;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to send..., trying again");
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

        public async Task<Guid> AddValue(int value)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(Urls[rand.Next(0, Urls.Length)]);
                    var result = await client.PostAsJsonAsync("api/values", value);
                    var text = (await result.Content.ReadAsStringAsync()).Replace("\"", "");

                    return new Guid(text);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public async Task<int> GetValue(Guid id)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(Urls[rand.Next(0, Urls.Length)]);
                    var result = await client.GetAsync("api/values/" + id);

                    var data = JObject.Parse(await result.Content.ReadAsStringAsync());

                    return data["data"]["data"].Value<int>();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public async Task<bool> UpdateValue(Guid id, int newValue)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(Urls[rand.Next(0, Urls.Length)]);
                    var result = await client.PutAsJsonAsync("api/values/" + id, newValue);
                    return true;
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
    }
}
