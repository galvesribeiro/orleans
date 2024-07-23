using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Serialization;

namespace Orleans.Streaming.NATS;

internal sealed class NatsQueueAdapterReceiver : IQueueAdapterReceiver
{
    private readonly ILogger _logger;
    private readonly int _partition;
    private readonly Serializer _serializer;
    private NatsConnectionManager? _nats;
    private NatsStreamConsumer? _consumer;
    private Task? _outstandingTask;

    public static IQueueAdapterReceiver Create(string providerName, ILoggerFactory loggerFactory, int partition,
        NATSOptions options, Serializer serializer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(options);

        var connectionManager = new NatsConnectionManager(providerName, loggerFactory, options);
        return new NatsQueueAdapterReceiver(providerName, loggerFactory, partition, connectionManager, serializer);
    }

    private NatsQueueAdapterReceiver(string providerName, ILoggerFactory loggerFactory, int partition,
        NatsConnectionManager nats, Serializer serializer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(nats);

        this._logger = loggerFactory.CreateLogger<NatsQueueAdapterReceiver>();
        this._nats = nats;
        this._partition = partition;
        this._serializer = serializer;
    }

    public async Task Initialize(TimeSpan timeout)
    {
        // If it is null, then we are shutting down
        if (this._nats is null) return;

        this._consumer = this._nats.CreateConsumer(this._partition);
        if (this._consumer is null)
        {
            this._logger.LogError("Unable to create consumer for partition {Partition}", this._partition);
            return;
        }

        using var cts = new CancellationTokenSource(timeout);
        await this._consumer.Initialize(cts.Token);
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        try
        {
            if (this._outstandingTask is not null)
            {
                await this._outstandingTask;
            }
        }
        finally
        {
            this._consumer = null;
            this._nats = null;
        }
    }

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        try
        {
            var consumer = this._consumer;
            if (consumer is null) return [];

            var task = consumer.GetMessages(maxCount);
            this._outstandingTask = task;
            var messages = await task;

            var containers = new List<IBatchContainer>();

            foreach (var message in messages)
            {
                var payload = this._serializer.Dese  message.Payload
                var container = new BatchContainer(message);
                containers.Add(container);
            }
        }
        finally
        {
            this._outstandingTask = null;
        }
    }

    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages) => throw new NotImplementedException();
}