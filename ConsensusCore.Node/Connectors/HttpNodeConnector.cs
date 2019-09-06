using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using ConsensusCore.Domain.BaseClasses;
using ConsensusCore.Domain.Models;
using ConsensusCore.Domain.RPCs;

namespace ConsensusCore.Node.Connectors
{
    public class HttpNodeConnector : IConnector
    {
        private HttpClient _httpClient;
        private HttpClient _dataClient;
        public string Url { get; set; }

        public HttpNodeConnector(string baseUrl, TimeSpan timeoutInterval, TimeSpan dataTimeoutInterval)
        {
            _httpClient = new HttpClient();
            _httpClient.Timeout = timeoutInterval;
            _httpClient.BaseAddress = new Uri(baseUrl);
            _dataClient = new HttpClient();
            _dataClient.Timeout = dataTimeoutInterval;
            _dataClient.BaseAddress = new Uri(baseUrl);
            Url = baseUrl;
        }

        public async Task<NodeInfo> GetNodeInfoAsync()
        {
            var result = await _httpClient.GetAsync("/api/node");

            if (result.IsSuccessStatusCode)
            {
                return JsonConvert.DeserializeObject<NodeInfo>(await result.Content.ReadAsStringAsync());
            }
            throw new Exception("Failed to get node info with result " + result.StatusCode + " and message " + await result.Content.ReadAsStringAsync());
        }

        public async Task<HttpResponseMessage> PostAsJsonAsync(HttpClient client, string url, object o)
        {
            return await _httpClient.PostAsync(url, new StringContent(JsonConvert.SerializeObject(o), Encoding.UTF8, "application/json"));
        }

        public async Task<TResponse> Send<TResponse>(IClusterRequest<TResponse> request) where TResponse : BaseResponse
        {
            HttpResponseMessage result;

            switch (request)
            {
                case WriteData t1:
                    result = await PostAsJsonAsync(_dataClient, "/api/node/RPC", request);
                    break;
                default:
                    result = await PostAsJsonAsync(_httpClient, "/api/node/RPC", request);
                    break;
            }

            try
            {
                if (result.IsSuccessStatusCode)
                {
                    return JsonConvert.DeserializeObject<TResponse>(await result.Content.ReadAsStringAsync());
                }
                else
                {
                    throw new Exception("Failed to send request with status code " + result.StatusCode + ".");
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
    }
}
