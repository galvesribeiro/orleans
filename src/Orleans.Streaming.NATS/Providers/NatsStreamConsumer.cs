using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Orleans.Streaming.NATS;

/// <summary>
/// Wrapper around a NATS JetStream consumer
/// </summary>
internal sealed class NatsStreamConsumer
{
    private readonly int _maxBatchSize;
    private readonly NatsJsonContextSerializer<NatsStreamMessage> _serializer;
    private readonly NatsJSContext _context;
    private readonly ILogger _logger;
    private readonly ConsumerConfig _config;
    private readonly string _streamName;
    private INatsJSConsumer? _consumer;

    public NatsStreamConsumer(
        ILoggerFactory loggerFactory,
        NatsJSContext context,
        string provider,
        string stream,
        int partition,
        int batchSize,
        NatsJsonContextSerializer<NatsStreamMessage> serializer)
    {
        this._logger = loggerFactory.CreateLogger<NatsStreamConsumer>();
        this._streamName = stream;
        this._maxBatchSize = batchSize;
        this._serializer = serializer;
        this._context = context;

        this._config = new ConsumerConfig($"orleans-{provider}-{stream}-{partition}")
        {
            FilterSubject = $"{provider}.{partition}.>",
            MaxBatch = batchSize,
            DeliverPolicy = ConsumerConfigDeliverPolicy.LastPerSubject,
            MaxAckPending = batchSize
        };
    }

    public async Task<NatsStreamMessage[]> GetMessages(int messageCount = 0,
        CancellationToken cancellationToken = default)
    {
        if (this._consumer is null)
        {
            this._logger.LogError("NATS Consumer is not initialized. Call Initialize() first.");
            return [];
        }

        var batchCount = messageCount > 0 && messageCount < this._maxBatchSize ? messageCount : this._maxBatchSize;
        var messages = ArrayPool<NatsStreamMessage>.Shared.Rent(batchCount);

        var i = 0;

        await foreach (var msg in this._consumer.FetchNoWaitAsync(
                               new NatsJSFetchOpts { MaxMsgs = batchCount, Expires = TimeSpan.FromSeconds(2) },
                               serializer: this._serializer)
                           .WithCancellation(cancellationToken))
        {
            var streamMessage = msg.Data;
            if (streamMessage is null)
            {
                this._logger.LogWarning("Unable to deserialize NATS message for subject {Subject}. Ignoring...",
                    msg.Subject);
                continue;
            }



            messages[i] = streamMessage;
            messages[i].JetStreamMessage = msg;
            i++;
        }

        return messages;
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        var consumer =
            await this._context.CreateOrUpdateConsumerAsync(this._streamName, this._config, cancellationToken);

        this._consumer = consumer;
    }
}