using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.Runtime;

namespace Orleans.Streaming.NATS;

internal class StreamIdJsonConverter : JsonConverter<StreamId>
{
    public override StreamId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            throw new JsonException("StreamId is not a string");
        }

        var str = reader.GetString();

        if (string.IsNullOrWhiteSpace(str))
        {
            throw new JsonException("StreamId is empty");
        }

        return StreamId.Parse(Convert.FromBase64String(str));
    }

    public override void Write(Utf8JsonWriter writer, StreamId value, JsonSerializerOptions options) =>
        writer.WriteStringValue(Convert.ToBase64String(value.FullKey.ToArray()));
}