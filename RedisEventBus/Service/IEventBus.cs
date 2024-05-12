using RedisEventBus.Events;
using System.IO.Compression;
using System.Text;

namespace RedisEventBus.Service
{
    public interface IEventBus
    {
        public const string GLOBAL_DEAD_LETTER_TOPIC = "global.error";
        public const int RE_SUBSCRIBE_DELAY_MS = 2000;
        public const int DEFAULT_TOPIC_MAXLEN = 10000;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Blocker Code Smell", "S2368:Public methods should not have multidimensional array parameters", Justification = "We are using this jagged array")]
        Task<string> PublishAsync(string topic, byte[][] message, int topicLength = DEFAULT_TOPIC_MAXLEN);
        Task<string> PublishAsync(string topic, string[] message, int topicLength = DEFAULT_TOPIC_MAXLEN);
        Task<string> PublishAsync<T>(string topic, ServiceEvent<T> auditEvent, int topicLength = DEFAULT_TOPIC_MAXLEN) where T : IServiceEventData, new();
        Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Action<byte[]> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = RE_SUBSCRIBE_DELAY_MS);
        Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Action<object?> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = RE_SUBSCRIBE_DELAY_MS);
        Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Action<string> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = RE_SUBSCRIBE_DELAY_MS);
        Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Func<byte[], Task> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = RE_SUBSCRIBE_DELAY_MS);
        Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Func<object?, Task> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = RE_SUBSCRIBE_DELAY_MS);
        Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Func<string, Task> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = RE_SUBSCRIBE_DELAY_MS);
        CancellationToken Subscribe(string topic, string globalUniqueConsumerGroup, string consumerId, Action<byte[]> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC);
        CancellationToken Subscribe(string topic, string globalUniqueConsumerGroup, string consumerId, Action<object?> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC);
        CancellationToken Subscribe(string topic, string globalUniqueConsumerGroup, string consumerId, Action<string> action, CancellationToken? cancellationToken = null, string deadLetterTopic = GLOBAL_DEAD_LETTER_TOPIC);

        public byte[] Compress(byte[]? data)
        {
            if (data == null || data.Length == 0)
            {
                return Array.Empty<byte>();
            }

            using var input = new MemoryStream(data);
            using var output = new MemoryStream();
            using var stream = new BrotliStream(output, CompressionLevel.Fastest);
            input.CopyTo(stream);
            stream.Close();
            return output.ToArray();
        }

        public byte[] Decompress(byte[]? data)
        {
            if (data == null || data.Length == 0)
            {
                return Array.Empty<byte>();
            }

            using var input = new MemoryStream(data);
            using var output = new MemoryStream();
            using var stream = new BrotliStream(input, CompressionMode.Decompress);
            stream.CopyTo(output);
            return output.ToArray();
        }

        public byte[] GetBytes(string data)
        {
            if (string.IsNullOrWhiteSpace(data))
            {
                return Array.Empty<byte>();
            }
            return Encoding.Default.GetBytes(data);
        }
    }
}