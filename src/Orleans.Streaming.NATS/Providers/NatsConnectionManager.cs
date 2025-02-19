using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Orleans.Streaming.NATS;

/// <summary>
/// Wrapper around a NATS and JetStream APIs
/// </summary>
internal sealed class NatsConnectionManager
{
    private static readonly NatsJsonContextSerializer<NatsStreamMessage> Serializer;
    private readonly string _providerName;
    private readonly NatsOpts _natsClientOptions;
    private readonly NatsConnection _natsConnection;
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly NATSOptions _options;
    private NatsJSContext? _natsContext;

    static NatsConnectionManager()
    {
        Serializer = new NatsJsonContextSerializer<NatsStreamMessage>(NatsSerializerContext.Default);
    }

    [GeneratedActivatorConstructor]
    public NatsConnectionManager(string providerName, ILoggerFactory loggerFactory, NATSOptions options)
    {
        this._providerName = providerName;
        this._loggerFactory = loggerFactory;
        this._logger = this._loggerFactory.CreateLogger<NatsConnectionManager>();
        this._options = options;
        this._natsClientOptions =
            this._options.NatsClientOptions ?? NatsOpts.Default with { Name = $"Orleans-{this._providerName}" };
        this._natsConnection = new NatsConnection(this._natsClientOptions);
    }

    /// <summary>
    /// Initialize the connection to the NATS server and check if JetStream is available
    /// </summary>
    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        await this._natsConnection.ConnectAsync();

        if (this._natsConnection.ConnectionState != NatsConnectionState.Open)
        {
            this._logger.LogError("Unable to connect to NATS server {NatsServer}", this._natsClientOptions.Url);
            return;
        }

        if (!this._natsConnection.ServerInfo!.JetStreamAvailable)
        {
            this._logger.LogError(
                "Unable to use {NatsServer} for Orleans Stream Provider {ProviderName}: JetStream is not available",
                this._natsClientOptions.Url, this._providerName);
            return;
        }

        this._logger.LogTrace("Connected to NATS server {NatsServer}", this._natsClientOptions.Url);

        this._natsContext = new NatsJSContext(this._natsConnection);

        var streamConfig = new StreamConfig(this._options.Stream, [$"{this._providerName}.>"])
        {
            SubjectTransform = new SubjectTransform
            {
                Src = $"{this._providerName}.*.*",
                Dest =
                    @$"{this._providerName}.{{partition({this._options.PartitionCount},1,2)}}.{{wildcard(1)}}.{{wildcard(2)}}"
            }
        };

        await this._natsContext.CreateStreamAsync(streamConfig, cancellationToken);

        this._logger.LogTrace(
            "Initialized to NATS JetStream stream {Stream} on server {NatsServer}",
            this._options.Stream,
            this._natsClientOptions.Url);
    }

    /// <summary>
    /// Enqueue a message to NATS JetStream stream
    /// </summary>
    /// <param name="message">The message</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task EnqueueMessage(NatsStreamMessage message, CancellationToken cancellationToken = default)
    {
        if (this._natsContext is null)
        {
            this._logger.LogError(
                "Unable to send message for {StreamId}: NATS context is not initialized",
                message.StreamId);
            return;
        }

        var subject = $"{this._providerName}.{message.StreamId.Namespace}.{message.StreamId.Key}";

        var ack = await this._natsContext.PublishAsync(subject, message, Serializer,
            cancellationToken: cancellationToken);
        ack.EnsureSuccess();
    }

    /// <summary>
    /// Create a NATS JetStream consumer
    /// </summary>
    /// <param name="partition">The partition number</param>
    /// <returns>A wrapper to a durable NATS JetStream stream consumer</returns>
    public NatsStreamConsumer? CreateConsumer(int partition)
    {
        if (this._natsContext is not null)
        {
            return new NatsStreamConsumer(this._loggerFactory, this._natsContext, this._providerName, this._options.Stream,
                partition, this._options.BatchSize, Serializer);
        }

        this._logger.LogError(
            "Unable to create consumer for {Stream}: NATS context is not initialized",
            this._options.Stream);

        return null!;
    }
}