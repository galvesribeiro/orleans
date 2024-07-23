using NATS.Client.Core;

namespace Orleans.Streaming.NATS;

/// <summary>
/// Configuration options for the NATS JetStream stream provider
/// </summary>
public class NATSOptions
{
    /// <summary>
    /// The NATS JetStream stream name
    /// </summary>
    public required string Stream { get; set; }

    /// <summary>
    /// Configuration options for the NATS client.
    /// If not provided, a default client will be created with the name Orleans-{providerName}
    /// and will connect to the NATS server at localhost:4222
    /// </summary>
    public NatsOpts? NatsClientOptions { get; set; }

    /// <summary>
    /// The maximum number of messages to fetch in a single batch.
    /// Defaults to 100.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// The number of partitions in the stream.
    /// Defaults to 4.
    /// </summary>
    public int PartitionCount { get; set; } = 4;
}