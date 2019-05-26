using ConsensusCore.Node.BaseClasses;
using ConsensusCore.Node.Enums;
using ConsensusCore.Node.Exceptions;
using ConsensusCore.Node.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace ConsensusCore.Node.Connectors
{
    public class HttpNodeConnector
    {
        private HttpClient _httpClient;

        public HttpNodeConnector(string baseUrl, TimeSpan timeoutInterval)
        {
            _httpClient = new HttpClient();
            _httpClient.Timeout = timeoutInterval;
            _httpClient.BaseAddress = new Uri(baseUrl);
        }

        public async Task<NodeInfo> GetNodeInfoAsync()
        {
            var result = await _httpClient.GetAsync("/api/node");

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<NodeInfo>(await result.Content.ReadAsStringAsync());
            }

            return null;
        }

        public async Task<HttpResponseMessage> PostAsJsonAsync(string url, object o)
        {
            return await _httpClient.PostAsync(url, new StringContent(JsonConvert.SerializeObject(o), Encoding.UTF8, "application/json"));
        }

        public async Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request)
        {
            var result = await PostAsJsonAsync("/api/node/RPC", request);

            if(result.IsSuccessStatusCode)
            {
                 return JsonConvert.DeserializeObject<TResponse>(await result.Content.ReadAsStringAsync());
            }

            throw new Exception("Failed to send request.");
        }

        /*

        public async Task<VoteReply> SendRequestVote(
             int Term,
             Guid CandidateId,
             int LastLogIndex,
             int LastLogTerm
            )
        {
            var result = await PostAsJsonAsync("/api/node/request-vote", new RequestVote()
            {
                CandidateId = CandidateId,
                LastLogIndex = LastLogIndex,
                Term = Term,
                LastLogTerm = LastLogTerm
            });

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<VoteReply>(await result.Content.ReadAsStringAsync());
            }
            return null;
        }

        public async Task<HttpResponseMessage> PostAsJsonAsync(string url, object o)
        {
            return await _httpClient.PostAsync(url, new StringContent(JsonConvert.SerializeObject(o), Encoding.UTF8, "application/json"));
        }

        public async Task<bool> SendAppendEntry(int term,
         Guid leaderId,
         int prevLogIndex,
         int prevLogTerm,
         List<LogEntry> entries,
         int leaderCommit)
        {
            var result = await PostAsJsonAsync("/api/node/append-entry", new AppendEntry()
            {
                Term = term,
                LeaderId = leaderId,
                PrevLogIndex = prevLogIndex,
                PrevLogTerm = prevLogTerm,
                Entries = entries,
                LeaderCommit = leaderCommit
            });

            if (result.IsSuccessStatusCode)
            {
                return true;
            }
            else if (result.StatusCode == HttpStatusCode.BadRequest)
            {
                var response = JsonConvert.DeserializeObject<InvalidAppendEntryResponse>(await result.Content.ReadAsStringAsync());
                if (response.ConflictName == AppendEntriesExceptionNames.ConflictingLogEntryException)
                {
                    throw new ConflictingLogEntryException()
                    {
                        ConflictingTerm = response.ConflictingTerm.Value,
                        FirstTermIndex = response.FirstTermIndex.Value
                    };
                }
                else if (response.ConflictName == AppendEntriesExceptionNames.MissingLogEntryException)
                {
                    throw new MissingLogEntryException()
                    {
                        LastLogEntryIndex = response.LastLogEntryIndex.Value
                    };
                }
            }

            return false;
        }

        public async Task<VoteReply> RouteCommand<T>(List<T> entry, bool waitForCommit = false)
        {
            var result = await PostAsJsonAsync("/api/node/routed-command" + (waitForCommit ? "?wait-for-commit=true" : ""), entry);

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<VoteReply>(await result.Content.ReadAsStringAsync());
            }

            return null;
        }

        public async Task<bool> AssignShardAsync(AssignDataShard assignDataShard)
        {
            var result = await PostAsJsonAsync("/api/node/assign-shard-command/" + assignDataShard.Type + "/" + assignDataShard.ShardId, assignDataShard);

            if (result.IsSuccessStatusCode)
            {
                return true;
            }

            return false;
        }

        public async Task<object> GetDataShard(string type,Guid shardId)
        {
            var result = await _httpClient.GetAsync("/api/node/shards" + type + "/" + shardId);
            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<object>(await result.Content.ReadAsStringAsync());
            }
            return null;
        }
        */
    }
}
