using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Streaming.NATS;

internal sealed class NatsQueueAdapter(
    string providerName,
    NATSOptions options,
    ILoggerFactory loggerFactory,
    HashRingBasedPartitionedStreamQueueMapper streamQueueMapper,
    Serializer serializer,
    NatsConnectionManager natsConnectionManager) : IQueueAdapter
{
    public string Name => providerName;
    public bool IsRewindable => false; // We will make it rewindable later
    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId) => throw new System.NotImplementedException();

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token,
        Dictionary<string, object> requestContext)
    {
        // We don't use the IBatchContainer so other systems outside Orleans using NATS can consume
        // them without care too much about the Orleans internals and without requiring a custom DataAdapter.
        var messages = events.Select(e => new NatsStreamMessage
        {
            StreamId = streamId,
            Payload = serializer.GetSerializer<T>().SerializeToArray(e),
            RequestContext = requestContext
        });

        foreach (var m in messages)
        {
            await natsConnectionManager.EnqueueMessage(m);
        }
    }
}