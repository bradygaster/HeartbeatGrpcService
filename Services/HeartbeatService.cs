using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using HeartbeatService;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace GrpcService
{
    public class HeartbeatService : Heartbeat.HeartbeatBase
    {
        string _cacheKey = "heartbeats";
        private readonly ILogger<HeartbeatService> _logger;
        private readonly IMemoryCache _memoryCache;
        public HeartbeatService(ILogger<HeartbeatService> logger, IMemoryCache memoryCache)
        {
            _memoryCache = memoryCache;
            _logger = logger;
        }

        private HeartbeatMessage GetHeartbeatCache()
        {
            return _memoryCache.Get<HeartbeatMessage>(_cacheKey);
        }

        private void SetHeartbeatCache(HeartbeatMessage message = null)
        {
            _memoryCache.Set(_cacheKey, message);
        }

        public override Task<HeartbeatReceivedResponse> ReceiveHeartbeat(
            HeartbeatMessage request, 
            ServerCallContext context)
        {
            _logger.LogInformation($"Heartbeat received from {request.HostName} at {request.HostTimeStamp}");

            SetHeartbeatCache(request);

            return Task.FromResult(
                new HeartbeatReceivedResponse
                {
                    Success = true
                });
        }

        public override async Task StreamHeartbeats(
            Empty _, 
            IServerStreamWriter<HeartbeatMessage> responseStream, 
            ServerCallContext context)
        {
            while (!context.CancellationToken.IsCancellationRequested)
            {
                var heartbeat = GetHeartbeatCache();

                if(heartbeat != null)
                {
                    _logger.LogInformation($"Sending heartbeat from {heartbeat.HostName}");

                    await responseStream.WriteAsync(heartbeat);
                    
                    SetHeartbeatCache();
                }

                await Task.Delay(200);
            }

            if (context.CancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("The client cancelled their request");
            }
        }
    }
}
