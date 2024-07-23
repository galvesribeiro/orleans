using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace NATS.Tests;

public class UnitTest1
{
    [Fact]
    public async Task Test1()
    {
        await using var nats = new NatsConnection();
        await nats.ConnectAsync();
        var js = new NatsJSContext(nats);

        await js.CreateStreamAsync(new StreamConfig("orleans-cluster-1", ["myStreamProvider.*.>"]));

        var cts = new CancellationTokenSource();

        var consumer = await js.CreateOrUpdateConsumerAsync(
            "orleans-cluster-1",
            new ConsumerConfig("Orleans-PollingAgent-1")
            {
                // -> nats server mappings "myStreamProvider.*.*" "myStreamProvider.{{partition(10,1,2)}}.{{wildcard(1)}}.{{wildcard(2)}}"
                // Filter here only for messages on the partition 0
                FilterSubject = "myStreamProvider.0.*.>",
                MaxBatch = 100,
                DeliverPolicy = ConsumerConfigDeliverPolicy.LastPerSubject
            },
            cts.Token);

        for (var i = 0; i < 100; i++)
        {
            var ack = await js.PublishAsync($"myStreamProvider.testStream.{i}", $"Hello, World! {i}",
                cancellationToken: cts.Token);
            ack.EnsureSuccess();
        }

        var msgs = new List<NatsJSMsg<string>>();
        await foreach (var msg in consumer
                           .FetchAsync<string>(opts: new NatsJSFetchOpts
                           {
                               MaxMsgs = 100, Expires = TimeSpan.FromSeconds(1)
                           })
                           .WithCancellation(cts.Token))
        {
            // Process message
            msgs.Add(msg);
            await msg.AckAsync(cancellationToken: cts.Token);
        }

        // This polling agent should have only messages from the partition 0
        Assert.NotEmpty(msgs);
        Assert.True(msgs.Count == 10);
        // docker run -d --name nats -p 4222:4222 -p 6222:6222 -p 8222:8222 nats:2.10-alpine -js
    }
}