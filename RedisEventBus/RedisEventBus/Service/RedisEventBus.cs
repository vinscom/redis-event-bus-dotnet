using JT.Common.Service;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RedisEventBus.Vinscom.RedisEventBus.Events;
using StackExchange.Redis;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;

namespace RedisEventBus.Vinscom.RedisEventBus.Service
{
    public partial class RedisEventBus : IEventBus
    {
        private readonly RedisValue REDIS_VALUE_KEY_NAME = new("event");
        private readonly JsonSerializerSettings _jsonSerializerSettings = new() { TypeNameHandling = TypeNameHandling.All };
        private readonly IDatabase _redisdb;
        private readonly ILogger? _logger;

        public RedisEventBus(string redisConfig, ILogger? logger = null)
        {
            _logger = logger;

            ConfigurationOptions configurationOptions = ConfigurationOptions.Parse(redisConfig);
            configurationOptions.AbortOnConnectFail = false;
            configurationOptions.ConnectRetry = 100;
            configurationOptions.ReconnectRetryPolicy = new LinearRetry(100);
            configurationOptions.BacklogPolicy = BacklogPolicy.FailFast;

            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);

            redis.ConnectionFailed += (sender, e) => _logger?.LogError(e.Exception, "-----> Connection Failed");
            redis.ConnectionRestored += (sender, e) => _logger?.LogInformation("-----> Connection Restored");
            redis.ErrorMessage += (sender, e) => _logger?.LogError("-----> " + e.Message);
            redis.InternalError += (sender, e) => _logger?.LogError(e.Exception, "-----> Internal Error");
            redis.ConfigurationChanged += (sender, e) => _logger?.LogInformation("-----> Configuration Changed");
            redis.HashSlotMoved += (sender, e) => _logger?.LogInformation("-----> Hash Slot Moved");
            redis.ConfigurationChangedBroadcast += (sender, e) => _logger?.LogInformation("-----> Configuration Changed Broadcast");
            redis.ServerMaintenanceEvent += (sender, e) => _logger?.LogInformation("-----> Server Maintenance Event");

            _redisdb = redis.GetDatabase();

        }

        public async Task<string> PublishAsync<T>(string topic, ServiceEvent<T> auditEvent, int topicLength = IEventBus.DEFAULT_TOPIC_MAXLEN) where T : IServiceEventData, new()
        {
            string message = JsonConvert.SerializeObject(auditEvent, _jsonSerializerSettings);
            return await PublishAsync(topic, new byte[][] { Encoding.Default.GetBytes(message) }, topicLength);
        }

        public async Task<string> PublishAsync(string topic, string[] message, int topicLength = IEventBus.DEFAULT_TOPIC_MAXLEN)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message), "Message List null");
            }

            var msgs = message.Select(x => ((IEventBus)this).GetBytes(x)).ToArray();
            return await PublishAsync(topic, msgs, topicLength);
        }

        public async Task<string> PublishAsync(string topic, byte[][] message, int topicLength = IEventBus.DEFAULT_TOPIC_MAXLEN)
        {
            var msgs = message.Select(x => new NameValueEntry(REDIS_VALUE_KEY_NAME, ((IEventBus)this).Compress(x))).ToArray();
            var redisValue = await _redisdb.StreamAddAsync(topic, msgs, null, topicLength, true);
            return redisValue.IsNull ? string.Empty : redisValue.ToString();
        }

        public CancellationToken Subscribe(string topic, string globalUniqueConsumerGroup, string consumerId, Action<byte[]> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC)
        {
            CancellationTokenSource cts = cancellationToken == null ? new CancellationTokenSource() : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken.Value);

            RedisKey redisValueDeadLetterTopic = new(deadLetterTopic);

            Func<string> info = () => $"[{Environment.CurrentManagedThreadId}:{topic}:{globalUniqueConsumerGroup}:{consumerId}]\t[{GetHashCode()}]";

            try
            {
                _redisdb.StreamCreateConsumerGroup(topic, globalUniqueConsumerGroup, "0-0", true);
            }
            catch (RedisServerException ex)
            {
                _logger?.LogInformation(ex, "[{i}]\t[Subscribe]\t[Topic Already Created:{topic}]]", info(), topic);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "[{i}]\t[Subscribe]\t[Error Creating Consumer Group:{topic}]]", info(), topic);
                cts.Cancel();
                return cts.Token;
            }

            var messagePendingGenerator = Observable
                  .Generate(new RedisMessageGenerator(_redisdb, topic, globalUniqueConsumerGroup, consumerId, true, cts.Token, 5, _logger),
                      (t) => t.HasMore(),
                      t => t,
                      t => t.Next());

            var messageGenerator = Observable
                  .Generate(new RedisMessageGenerator(_redisdb, topic, globalUniqueConsumerGroup, consumerId, false, cts.Token, 5, _logger),
                      (t) =>
                      {
                          if (cts.Token.IsCancellationRequested)
                          {
                              _logger?.LogTrace("[{i}]\t[Subscribe]\t[CTS Cancel Generator{cts}]", info(), cts.GetHashCode());
                              return false;
                          }

                          if (t.HasMore())
                          {
                              return true;
                          }

                          Thread.Sleep(1000);
                          return true;
                      },
                      t => t,
                      t => t.Next());

            messagePendingGenerator                .Concat(messageGenerator)
                .SubscribeOn(NewThreadScheduler.Default)
                .Do(async t =>
                {
                    if (t == null)
                    {
                        return;
                    }

                    byte[]? rv = t.HasValue ? t.Value[REDIS_VALUE_KEY_NAME] : Array.Empty<byte>();

                    try
                    {
                        action(((IEventBus)this).Decompress(rv));
                        await _redisdb.StreamAcknowledgeAsync(topic, globalUniqueConsumerGroup, t.Value.Id);
                        _logger?.LogTrace("[{i}]\t[Subscribe]\t[Acknowledged]\t{id} on {topic}", info(), t.Value.Id, topic);
                    }
                    catch (Exception e) when (e is EventProcessingException || e is JsonSerializationException)
                    {
                        await _redisdb.StreamAddAsync(redisValueDeadLetterTopic, REDIS_VALUE_KEY_NAME, t.Value[REDIS_VALUE_KEY_NAME], null, IEventBus.DEFAULT_TOPIC_MAXLEN, true);
                        await _redisdb.StreamAcknowledgeAsync(topic, globalUniqueConsumerGroup, t.Value.Id);
                        _logger?.LogTrace("[{i}]\t[Subscribe]\t[Error Processing Event, Send to DLQ Topic:{dlq},Item:{id}]", info(), redisValueDeadLetterTopic, t.Value.Id);
                        _logger?.LogTrace("[{i}]\t[Subscribe]\t[Acknowledged]\t{id} on {topic}", info(), t.Value.Id, topic);
                    }
                    catch (Exception err)
                    {
                        _logger?.LogTrace(err, "[{i}]\t[Subscribe]\t[Error Processing Event, Unsubscribing by Cancelling{cts}. Item:{id}]", info(), cts.GetHashCode(), t.Value.Id);
                        cts.Cancel();
                    }
                })
                .Subscribe((t) => { }, err =>
                   {
                       _logger?.LogTrace(err, "[{i}]\t[Subscribe]\t[Error Processing Event, Unsubscribing by Cancelling{cts}.", info(), cts.GetHashCode());
                       cts.Cancel();

                   }, cts.Token);

            return cts.Token;
        }

        public CancellationToken Subscribe(string topic, string globalUniqueConsumerGroup, string consumerId, Action<string> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC)
        {
            return Subscribe(topic, globalUniqueConsumerGroup, consumerId, (t) =>
            {
                action(Encoding.Default.GetString(t));
            }, cancellationToken, deadLetterTopic);
        }

        public CancellationToken Subscribe(string topic, string globalUniqueConsumerGroup, string consumerId, Action<object?> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC)
        {
            return Subscribe(topic, globalUniqueConsumerGroup, consumerId, (string t) =>
            {
                var obj = JsonConvert.DeserializeObject(t, _jsonSerializerSettings);
                action(obj);
            }, cancellationToken, deadLetterTopic);
        }

        public async Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Action<byte[]> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = IEventBus.RE_SUBSCRIBE_DELAY_MS)
        {
            CancellationToken localCancellationToken = cancellationToken == null ? new CancellationTokenSource().Token : cancellationToken.Value;

            CancellationToken subscriberToken = Subscribe(topic, globalUniqueConsumerGroup, consumerId, action, localCancellationToken, deadLetterTopic);

            while (true)
            {
                if (subscriberToken.IsCancellationRequested && !localCancellationToken.IsCancellationRequested)
                {
                    subscriberToken = Subscribe(topic, globalUniqueConsumerGroup, consumerId, action, localCancellationToken, deadLetterTopic);
                }

                if (localCancellationToken.IsCancellationRequested)
                {
                    break;
                }
                await Task.Delay(reSubscribeDelayMs);
            }
        }

        public Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Action<string> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = IEventBus.RE_SUBSCRIBE_DELAY_MS)
        {
            return SubscribeWait(topic, globalUniqueConsumerGroup, consumerId, (t) =>
            {
                action(Encoding.Default.GetString(t));
            }, cancellationToken, deadLetterTopic, reSubscribeDelayMs);
        }

        public Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Action<object?> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = IEventBus.RE_SUBSCRIBE_DELAY_MS)
        {
            return SubscribeWait(topic, globalUniqueConsumerGroup, consumerId, (string t) =>
            {
                var obj = JsonConvert.DeserializeObject(t, _jsonSerializerSettings);
                action(obj);
            }, cancellationToken, deadLetterTopic, reSubscribeDelayMs);
        }

        public Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Func<byte[], Task> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = IEventBus.RE_SUBSCRIBE_DELAY_MS)
        {
            return SubscribeWait(topic, globalUniqueConsumerGroup, consumerId, (t) =>
            {
                try
                {
                    Task.Run(async () => await action(t)).Wait();
                }
                catch (AggregateException e)
                {
                    _logger?.LogError(e, "Aggregate Exception");
                    if (e.InnerException != null)
                    {
                        throw e.InnerException;
                    }

                }
            }, cancellationToken, deadLetterTopic, reSubscribeDelayMs);
        }

        public Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Func<string, Task> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = IEventBus.RE_SUBSCRIBE_DELAY_MS)
        {
            return SubscribeWait(topic, globalUniqueConsumerGroup, consumerId, (t) =>
            {
                try
                {
                    Task.Run(async () => await action(t)).Wait();
                }
                catch (AggregateException e)
                {
                    _logger?.LogError(e, "Aggregate Exception");
                    if (e.InnerException != null)
                    {
                        throw e.InnerException;
                    }
                }
            }, cancellationToken, deadLetterTopic, reSubscribeDelayMs);
        }

        public Task SubscribeWait(string topic, string globalUniqueConsumerGroup, string consumerId, Func<object?, Task> action, CancellationToken? cancellationToken = null, string deadLetterTopic = IEventBus.GLOBAL_DEAD_LETTER_TOPIC, int reSubscribeDelayMs = IEventBus.RE_SUBSCRIBE_DELAY_MS)
        {
            return SubscribeWait(topic, globalUniqueConsumerGroup, consumerId, (t) =>
            {
                try
                {
                    Task.Run(async () => await action(t)).Wait();
                }
                catch (AggregateException e)
                {
                    _logger?.LogError(e, "Aggregate Exception");
                    if (e.InnerException != null)
                    {
                        throw e.InnerException;
                    }
                }
            }, cancellationToken, deadLetterTopic, reSubscribeDelayMs);
        }
    }

}