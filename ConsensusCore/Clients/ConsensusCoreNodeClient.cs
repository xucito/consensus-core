using ConsensusCore.Messages;
using ConsensusCore.ViewModels;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace ConsensusCore.Clients
{
    public static class ConsensusCoreNodeClient
    {
        private static HttpClient HttpClient;

        static ConsensusCoreNodeClient()
        {
        }

        public static void SetTimeout(TimeSpan timeoutInterval)
        {
            HttpClient = new HttpClient();
            HttpClient.Timeout = timeoutInterval;
        }

        public static async Task<NodeInfo> GetNodeInfoAsync(string url)
        {
            var result = await HttpClient.GetAsync(url + "/api/node");

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<NodeInfo>(await result.Content.ReadAsStringAsync());
            }

            return null;
        }

        public static async Task<VoteReply> SendRequestVote(string url, RequestVote vote)
        {
            var result = await HttpClient.PostAsJsonAsync(url + "/api/node/request-vote", vote);

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<VoteReply>(await result.Content.ReadAsStringAsync());
            }

            return null;
        }

        public static async Task<VoteReply> SendAppendEntry(string url, AppendEntry entry)
        {
            var result = await HttpClient.PostAsJsonAsync(url + "/api/node/append-entry", entry);

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<VoteReply>(await result.Content.ReadAsStringAsync());
            }

            return null;
        }
    }
}
