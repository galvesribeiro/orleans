using System;
using System.Linq;
using System.Collections.Generic;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.NATS;

[Serializable]
[GenerateSerializer]
public class NatsBatchContainer : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; set; }

    [Id(1)]
    public StreamSequenceToken SequenceToken { get; set; }

    [Id(2)]
    public List<object> Events { get; set; }

    [Id(3)]
    public Dictionary<string, object>? RequestContext { get; set; }

    public NatsBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object>? requestContext,
        StreamSequenceToken sequenceToken)
    {
        this.StreamId = streamId;
        this.Events = events;
        this.RequestContext = requestContext;
        this.SequenceToken = sequenceToken;
    }

    public bool ImportRequestContext()
    {
        if (this.RequestContext is not null)
        {
            RequestContextExtensions.Import(this.RequestContext);
            return true;
        }

        return false;
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() =>
        this.Events.OfType<T>().Select((e, i) => Tuple.Create(e, this.SequenceToken));
}