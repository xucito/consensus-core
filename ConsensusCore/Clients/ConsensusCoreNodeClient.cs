using ConsensusCore.BaseClasses;
using ConsensusCore.Exceptions;
using ConsensusCore.Messages;
using ConsensusCore.Utility;
using ConsensusCore.ViewModels;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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

        public static async Task<bool> SendAppendEntry<T>(string url, AppendEntry<T> entry) where T: BaseCommand
        {
            var result = await HttpClient.PostAsJsonAsync(url + "/api/node/append-entry", entry);

            if (result.IsSuccessStatusCode)
            {
                return true;
            }
            else if(result.StatusCode == HttpStatusCode.BadRequest)
            {
                var response = JsonConvert.DeserializeObject<InvalidAppendEntryResponse>(await result.Content.ReadAsStringAsync());
                if (response.ConflictName == AppendEntriesExceptionNames.ConflictingLogEntryException)
                {
                    throw new ConflictingLogEntryException(){
                        ConflictingTerm = response.ConflictingTerm.Value,
                        FirstTermIndex = response.FirstTermIndex.Value
                    };
                }
                else if(response.ConflictName == AppendEntriesExceptionNames.MissingLogEntryException)
                {
                    throw new MissingLogEntryException()
                    {
                        LastLogEntryIndex = response.LastLogEntryIndex.Value
                    };
                }
            }

            return false;
        }

        public static async Task<VoteReply> RouteCommands<T>(string url, List<T> entry)
        {
            var result = await HttpClient.PostAsJsonAsync(url + "/api/node/routed-command", entry);

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<VoteReply>(await result.Content.ReadAsStringAsync());
            }

            return null;
        }
    }
}
