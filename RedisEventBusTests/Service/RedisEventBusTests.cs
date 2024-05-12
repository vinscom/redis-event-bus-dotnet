using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using JT.Common.Events.Twilio;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using RedisEventBus.Events;
using RedisEventBus.Service;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace JT.Common.Service.Tests
{
    [TestClass()]
    public class RedisEventBusTests
    {
#pragma warning disable CS8618
        private static IEventBus _eventBus;
        private static RedisTestcontainer _container;
        private static ILogger<RedisEventBus> _logger;
#pragma warning restore CS8618

        private static readonly TestcontainerDatabaseConfiguration configuration = new RedisTestcontainerConfiguration();
        public static ILoggerFactory LogFactory { get; } = LoggerFactory.Create(builder =>
                {
                    builder.AddSimpleConsole(options =>
                    {
                        options.IncludeScopes = true;
                        options.SingleLine = true;
                        options.TimestampFormat = "hh:mm:ss ";
                    }).SetMinimumLevel(LogLevel.Trace);
                });

        public static ILogger<T> CreateLogger<T>() => LogFactory.CreateLogger<T>();

        [ClassInitialize]
        public static async Task SetupAsync(TestContext context)
        {
            _logger = CreateLogger<RedisEventBus>();
            _container = new TestcontainersBuilder<RedisTestcontainer>().WithDatabase(configuration).Build();
            await _container.StartAsync();
            _eventBus = new RedisEventBus(_container.ConnectionString, _logger);
        }

        [ClassCleanup]
        public static async Task TeardownAsync()
        {
            if (_container != null)
            {
                await _container.DisposeAsync();
            }
        }

        [TestMethod("Test Compression and Decpression of JSON message")]
        public void CompressDecompressTest()
        {
            var mock = new Mock<IEventBus>(MockBehavior.Loose) { CallBase = true };
            const string data =
                @"{
                    ""$type"": ""JT.Common.Events.AuditEvent`1[[JT.Common.Events.Twilio.TwilioRecordingReady, JTCommonEvents]], JTCommonEvents"",
                    ""Id"": """",
                    ""Type"": ""jt_twilioconnector_twiliorecordingready"",
                    ""SpecVersion"": """",
                    ""Source"": """",
                    ""CustomerId"": """",
                    ""TimeStamp"": ""0001-01-01T00:00:00"",
                    ""Properties"": {
                        ""$type"": ""JT.Common.Events.Twilio.TwilioRecordingReady, JTCommonEvents"",
                        ""CreatedOn"": ""2022-04-03T15:37:21.8739101Z""
                    }
                }";

            var cdata = mock.Object.Compress(Encoding.UTF8.GetBytes(data));
            var udata = mock.Object.Decompress(cdata);
            Assert.AreEqual(data, Encoding.UTF8.GetString(udata));

            var compressNull = mock.Object.Compress(null);
            Assert.AreEqual(Array.Empty<byte>(), compressNull);

            var decompressNull = mock.Object.Decompress(compressNull);
            Assert.AreEqual(Array.Empty<byte>(), decompressNull);

            Assert.AreEqual(compressNull, decompressNull);
        }

        [TestMethod("Deserialize JSON to Class Object")]
        public void JsonSerializeAndDeserialize()
        {
            var e = new ServiceEvent<TwilioRecordingReady>();
            var s = JsonConvert.SerializeObject(e, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All });
            var d = JsonConvert.DeserializeObject(s, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All });

            switch (d)
            {
                case ServiceEvent<TwilioRecordingReady> _:
                    break;
                default:
                    Assert.Fail();
                    break;
            }
        }

        [TestMethod("Test Publishing of Object to Topic")]
        public async Task PublishAsync1()
        {
            const string deadletterQueue = "error.PublishAsync1";

            await _eventBus.PublishAsync("PublishAsync1", new ServiceEvent<TwilioRecordingReady>());

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("PublishAsync1", "PublishAsync1", "PublishAsync1", (byte[] t) =>
              {
                  Assert.IsTrue(t.Length > 0);
                  completion.Signal();
              }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("Test Publishing of String to Topic")]
        public async Task PublishAsync2()
        {
            const string deadletterQueue = "error.PublishAsync2";

            string message = JsonConvert.SerializeObject(new { name = "hello" });
            await _eventBus.PublishAsync("PublishAsync2", new string[] { message });

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("PublishAsync2", "PublishAsync2", "PublishAsync2", (byte[] t) =>
            {
                Assert.IsTrue(t.Length > 0);
                completion.Signal();
            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("Test Publishing of Byte to Topic")]
        public async Task PublishAsync3()
        {
            const string deadletterQueue = "error.PublishAsync3";

            byte[] message = Encoding.Default.GetBytes(JsonConvert.SerializeObject(new { name = "hello" }));
            await _eventBus.PublishAsync("PublishAsync3", new byte[][] { message });

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("PublishAsync3", "PublishAsync3", "PublishAsync3", (byte[] t) =>
            {
                Assert.AreEqual(message.Length, t.Length);
                completion.Signal();
            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("Subscribe topic to get string messages")]
        public async Task Subscribe1()
        {
            const string deadletterQueue = "error.Subscribe1";

            string message = "hello";
            await _eventBus.PublishAsync("Subscribe1", new string[] { message });

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("Subscribe1", "Subscribe1", "Subscribe1", (string t) =>
            {
                Assert.AreEqual(message, t);
                completion.Signal();
            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("Subscribe topic to get byte messages")]
        public async Task Subscribe2()
        {
            const string deadletterQueue = "error.Subscribe2";

            byte[] message = Encoding.Default.GetBytes("hello");
            await _eventBus.PublishAsync("Subscribe2", new byte[][] { message });

            CancellationTokenSource sub = new();

            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("Subscribe2", "Subscribe2", "Subscribe2", (byte[] t) =>
            {
                Assert.AreEqual(message.Length, t.Length);
                completion.Signal();
            }, sub.Token, deadletterQueue);
            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("Subscribe topic to get object messages")]
        public async Task Subscribe3()
        {
            const string deadletterQueue = "error.Subscribe3";

            var message = new ServiceEvent<TwilioRecordingReady>();
            await _eventBus.PublishAsync("Subscribe3", message);

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("Subscribe3", "Subscribe3", "Subscribe3", (object? t) =>
            {
                switch (t)
                {
                    case ServiceEvent<TwilioRecordingReady> _:
                        completion.Signal();
                        break;
                    default:
                        Assert.Fail();
                        break;
                }
            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("Validate StackExchange redis not working scenario")]
        public async Task RedisNotWorkingAsync()
        {
            var container1 = new TestcontainersBuilder<RedisTestcontainer>().WithDatabase(new RedisTestcontainerConfiguration() { Port = 16379 }).Build();
            await container1.StartAsync();

            RedisEventBus eb = new(container1.ConnectionString);
            string id1 = await eb.PublishAsync("RedisNotWorkingAsync", new ServiceEvent<TwilioRecordingReady>());
            Assert.IsNotNull(id1);

            await container1.DisposeAsync();

            await Assert.ThrowsExceptionAsync<RedisConnectionException>(async () =>
            {
                await eb.PublishAsync("RedisNotWorkingAsync", new ServiceEvent<TwilioRecordingReady>());
            });

            var container2 = new TestcontainersBuilder<RedisTestcontainer>().WithDatabase(new RedisTestcontainerConfiguration() { Port = 16379 }).Build();
            await container2.StartAsync();

            await Task.Delay(5000);

            string id2 = await eb.PublishAsync("RedisNotWorkingAsync", new ServiceEvent<TwilioRecordingReady>());
            Assert.IsNotNull(id2);

            await container2.DisposeAsync();
        }

        [TestMethod("Send 10 message and read 10 messages")]
        public async Task PubSub1()
        {
            const string callingMethodName = "PubSub1";
            const string deadletterQueue = "error.PubSub1";

            for (int i = 0; i < 10; i++)
            {
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello" });
            }

            CancellationTokenSource sub = new();

            var counter1 = new CountdownEvent(10);

            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter1.Signal();

            }, sub.Token, deadletterQueue);

            counter1.Wait();
            sub.Cancel();

            Assert.AreEqual(0, counter1.CurrentCount);

            CancellationTokenSource sub1 = new();

            var counter2 = new CountdownEvent(1);
            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter2.Signal();
            }, sub1.Token, deadletterQueue);

            counter2.Wait(2000);
            sub1.Cancel();
            Assert.AreEqual(1, counter2.CurrentCount);
        }

        [TestMethod("Process pending messages because of exception during previous processing")]
        public async Task PubSub2()
        {
            const string callingMethodName = "PubSub2";
            const string deadletterQueue = "error.PubSub2";

            for (int i = 0; i < 10; i++)
            {
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello" });
            }

            CancellationTokenSource sub = new();

            var counter1 = new CountdownEvent(10);
            var subCancel = _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
              {
                  counter1.Signal();
                  throw new ArgumentException("Manual exception");
              }, sub.Token, deadletterQueue);


            subCancel.WaitHandle.WaitOne();
            sub.Cancel();

            Assert.AreEqual(9, counter1.CurrentCount);

            CancellationTokenSource sub1 = new();
            var counter2 = new CountdownEvent(10);

            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
             {
                 counter2.Signal();
             }, sub1.Token, deadletterQueue);

            counter2.Wait();
            sub1.Cancel();

            Assert.AreEqual(0, counter2.CurrentCount);

            CancellationTokenSource sub2 = new();

            var counter3 = new CountdownEvent(1);
            _eventBus.Subscribe(deadletterQueue, callingMethodName, callingMethodName, (string t) =>
           {
               counter3.Signal();
           }, sub2.Token);

            counter3.Wait(2000);
            sub2.Cancel();

            Assert.AreEqual(1, counter3.CurrentCount);
        }

        [TestMethod("Send message to dead letter queue")]
        public async Task PubSub3()
        {
            const string callingMethodName = "PubSub3";
            const string deadletterQueue = "error.PubSub3";

            for (int i = 0; i < 10; i++)
            {
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello-" + i });
            }

            CancellationTokenSource sub1 = new();
            var counter1 = new CountdownEvent(10);
            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter1.Signal();
                throw new EventProcessingException();
            }, sub1.Token, deadletterQueue);

            counter1.Wait();
            sub1.Cancel();
            Assert.AreEqual(0, counter1.CurrentCount);

            //It take some time for aknowledged message updated in redis
            await Task.Delay(1000);
            CancellationTokenSource sub2 = new();
            var counter2 = new CountdownEvent(1);
            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter2.Signal();
            }, sub2.Token, deadletterQueue);

            counter2.Wait(1000);
            sub2.Cancel();

            Assert.AreEqual(1, counter2.CurrentCount);

            CancellationTokenSource sub3 = new();
            var counter3 = new CountdownEvent(10);
            _eventBus.Subscribe(deadletterQueue, callingMethodName, callingMethodName, (string t) =>
            {
                counter3.Signal();
            }, sub3.Token);

            counter3.Wait();
            sub3.Cancel();

            Assert.AreEqual(0, counter3.CurrentCount);
        }

        [TestMethod("Read messages pending for processing because of random processing error")]
        public async Task PubSub4()
        {
            string callingMethodName = "PubSub4";
            string deadletterQueue = "error.PubSub4";

            for (int i = 0; i < 2; i++)
            {
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello-" + i });
            }

            CancellationTokenSource cts1 = new();
            CancellationToken? cts2 = null;
            var counter1 = new CountdownEvent(3);

            while (true)
            {
                if (cts2 == null || cts2.Value.IsCancellationRequested)
                {
                    cts2 = _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
                    {
                        if (counter1.CurrentCount % 2 == 0)
                        {
                            counter1.Signal();
                            _logger.LogDebug("[{topic}:{gn}:{cn}]\t[{hash}]\t[ArgumentException:Counter{count}]\t{t}", callingMethodName, callingMethodName, callingMethodName, GetHashCode(), counter1.CurrentCount, t);
                            throw new ArgumentException("PubSub4 User Argument Exception");
                        }
                        else
                        {
                            counter1.Signal();
                            _logger.LogDebug("[{topic}:{gn}:{cn}]\t[{hash}]\t[EventProcessingException:Counter{count}]\t{t}", callingMethodName, callingMethodName, callingMethodName, GetHashCode(), counter1.CurrentCount, t);
                            throw new EventProcessingException();
                        }
                    }, cts1.Token, deadletterQueue);
                }

                counter1.Wait(1000);

                if (counter1.IsSet)
                    break;
            }

            cts1.Cancel();
            Assert.AreEqual(0, counter1.CurrentCount);

            CancellationTokenSource sub2 = new();
            var counter2 = new CountdownEvent(1);
            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter2.Signal();
            }, sub2.Token, deadletterQueue);

            counter2.Wait(1000);
            sub2.Cancel();
            Assert.AreEqual(1, counter2.CurrentCount);

            CancellationTokenSource sub3 = new();
            var counter3 = new CountdownEvent(2);
            _eventBus.Subscribe(deadletterQueue, callingMethodName, callingMethodName, (string t) =>
            {
                counter3.Signal();
            }, sub3.Token);

            counter3.Wait();
            sub3.Cancel();

            Assert.AreEqual(0, counter3.CurrentCount);
        }

        [TestMethod("SubscribeWait topic to get string messages throw exception to auto subscribe and cancel parent after 3")]
        public async Task SubscribeWait1()
        {
            const string deadletterQueue = "error.SubscribeWait1";

            byte[] message = Encoding.Default.GetBytes("hello");
            await _eventBus.PublishAsync("SubscribeWait1", new byte[][] { message });

            CancellationTokenSource sub = new();

            var counter1 = new CountdownEvent(3);
            await _eventBus.SubscribeWait("SubscribeWait1", "SubscribeWait1", "SubscribeWait1", (byte[] t) =>
            {
                Assert.AreEqual(message.LongLength, t.Length);

                if (counter1.CurrentCount == 0)
                {
                    sub.Cancel();
                }
                counter1.Signal();
                throw new ArgumentException("User Exception");

            }, sub.Token, deadletterQueue, 2000);

        }

        [TestMethod("SubscribeWait topic to get string messages throw exception to auto subscribe and cancel parent after 3")]
        public async Task SubscribeWait2()
        {
            const string deadletterQueue = "error.SubscribeWait2";

            await _eventBus.PublishAsync("SubscribeWait2", new string[] { "hello" });

            CancellationTokenSource sub = new();
            var counter1 = new CountdownEvent(3);

            await _eventBus.SubscribeWait("SubscribeWait2", "SubscribeWait2", "SubscribeWait2", (string t) =>
            {
                Assert.AreEqual("hello", t);

                if (counter1.CurrentCount == 0)
                {
                    sub.Cancel();
                }
                counter1.Signal();
                throw new ArgumentException("User Exception");
            }, sub.Token, deadletterQueue);
        }

        [TestMethod("SubscribeWait topic to get object messages throw exception to auto subscribe and cancel parent after 3")]
        public async Task SubscribeWait3()
        {
            const string deadletterQueue = "error.SubscribeWait3";

            var message = new ServiceEvent<TwilioRecordingReady>
            {
                SubjectId = "SubscribeWait3"
            };

            await _eventBus.PublishAsync("SubscribeWait3", message);

            CancellationTokenSource sub = new();
            var counter1 = new CountdownEvent(3);

            await _eventBus.SubscribeWait("SubscribeWait3", "SubscribeWait3", "SubscribeWait3", (object? t) =>
            {
                switch (t)
                {
                    case ServiceEvent<TwilioRecordingReady> msg:
                        Assert.AreEqual("SubscribeWait3", msg.SubjectId);
                        counter1.Signal();
                        break;
                    default:
                        Assert.Fail();
                        break;
                }

                if (counter1.CurrentCount == 0)
                {
                    sub.Cancel();
                }
                throw new ArgumentException("User Exception");
            }, sub.Token, deadletterQueue);
        }

        [TestMethod("SubscribeWait Deserialize Error Test, Expected: message of type Object class removed or renamed it shoud be moved to DeadLetterQueue")]
        public async Task SubscribeWaitDeserializeTest()
        {
            var serliazerSetting = new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All };

            const string callingMethodName = "SubscribeWaitDeserializeTest";
            const string deadletterQueue = "error.SubscribeWaitDeserializeTest";

            var message = new ServiceEvent<TwilioRecordingReady>
            {
                SubjectId = callingMethodName
            };

            string messageJson = JsonConvert.SerializeObject(message, serliazerSetting);

            messageJson = messageJson.Replace("JT.Common.Events.Twilio.TwilioRecordingReady", "JT.Common.Events.Editor.EditorActive");

            await _eventBus.PublishAsync(callingMethodName, new byte[][] { Encoding.Default.GetBytes(messageJson) });

            CancellationTokenSource sub = new();
            var counter1 = new CountdownEvent(1);

            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (object? t) =>
           {
               switch (t)
               {
                   case ServiceEvent<TwilioRecordingReady> msg:
                       Assert.AreEqual(callingMethodName, msg.SubjectId);
                       counter1.Signal();
                       break;

               }

           }, sub.Token, deadletterQueue);
            counter1.Wait(1000);
            sub.Cancel();

            Assert.AreEqual(1, counter1.CurrentCount);

            CancellationTokenSource sub3 = new();
            var counter3 = new CountdownEvent(1);
            _eventBus.Subscribe(deadletterQueue, callingMethodName, callingMethodName, (string t) =>
            {
                try
                {
                    JsonConvert.DeserializeObject<ServiceEvent<TwilioRecordingReady>>(t, serliazerSetting);
                    Assert.Fail();
                }
                catch (Exception ex)
                {
                    Assert.IsTrue(ex is JsonSerializationException);
                }

                messageJson = t.Replace("JT.Common.Events.Editor.EditorActive", "JT.Common.Events.Twilio.TwilioRecordingReady");
                var obj = JsonConvert.DeserializeObject<ServiceEvent<TwilioRecordingReady>>(messageJson, serliazerSetting);
                switch (obj)
                {
                    case ServiceEvent<TwilioRecordingReady> _:
                        counter3.Signal();
                        break;
                    default:
                        Assert.Fail();
                        break;
                }

            }, sub3.Token);

            counter3.Wait();
            sub3.Cancel();

            Assert.AreEqual(0, counter3.CurrentCount);
        }

        [TestMethod]
        public async Task NullValueProcessingTestAsync()
        {
            const string deadletterQueue = "error.NullValueProcessingTestAsync";

            ServiceEvent<TwilioRecordingReady>? evt = null;
#pragma warning disable CS8604 // This test is meant to test if values are null.
            await _eventBus.PublishAsync("NullObjectValueProcessingTestAsync", evt);
#pragma warning restore CS8604 // Possible null reference argument.

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            _eventBus.Subscribe("NullObjectValueProcessingTestAsync", "NullValueProcessingTestAsync", "NullValueProcessingTestAsync", (object? t) =>
            {
                switch (t)
                {
                    case ServiceEvent<TwilioRecordingReady> msg:
                        Assert.Fail();
                        completion.Signal();
                        break;
                    default:
                        Assert.IsNull(t);
                        completion.Signal();
                        break;
                }

            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();

            Assert.AreEqual(0, completion.CurrentCount);

#pragma warning disable CS8625 // This test is meant to test if values are null.
            await _eventBus.PublishAsync("NullStringValueProcessingTestAsync", new string[] { null });
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.

            CancellationTokenSource sub1 = new();
            var completion1 = new CountdownEvent(1);
            _eventBus.Subscribe("NullStringValueProcessingTestAsync", "NullValueProcessingTestAsync", "NullValueProcessingTestAsync", (string t) =>
            {
                Assert.IsTrue(t == string.Empty);
                completion1.Signal();
            }, sub1.Token, deadletterQueue);

            completion1.Wait();
            sub1.Cancel();

            Assert.AreEqual(0, completion1.CurrentCount);
        }

        [TestMethod("The sequence of communications is tracked using this test. Async/await has been deleted owing to a sequence problem. When employing async methods, multiple messages are delivered concurrently. Test: Send 1000 message and read 1000 messages.")]
        public async Task PubSubSequenceTest()
        {
            const string callingMethodName = "PubSubSequenceTest";
            const string deadletterQueue = "error.PubSubSequenceTest";
            var listPost = new Stack<int>();
            var listReceive = new Stack<int>();
            int messageCount = 100;
            for (int i = 0; i < messageCount; i++)
            {
                listPost.Push(i);
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello" });
            }

            CancellationTokenSource sub = new();
            var counter1 = new CountdownEvent(messageCount);
            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                listReceive.Push(listPost.Pop());
                counter1.Signal();

            }, sub.Token, deadletterQueue);

            counter1.Wait();
            sub.Cancel();

            Assert.AreEqual(0, listPost.Count);
            Assert.AreEqual(messageCount, listReceive.Count);
            Assert.AreEqual(0, counter1.CurrentCount);

            CancellationTokenSource sub1 = new();

            var counter2 = new CountdownEvent(1);
            _eventBus.Subscribe(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter2.Signal();
            }, sub1.Token, deadletterQueue);

            counter2.Wait(2000);
            sub1.Cancel();
            Assert.AreEqual(1, counter2.CurrentCount);
        }

        [TestMethod("SubscribeWait async topic to get string messages")]
        public async Task SubscribeWaitAsync1()
        {
            const string deadletterQueue = "error.SubscribeWaitAsync1";

            string message = "hello";
            await _eventBus.PublishAsync("SubscribeWaitAsync1", new string[] { message });

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            await _eventBus.SubscribeWait("SubscribeWaitAsync1", "SubscribeWaitAsync1", "SubscribeWaitAsync1", (string t) =>
             {
                 Assert.AreEqual(message, t);
                 completion.Signal();
                 if (completion.CurrentCount == 0)
                 {
                     sub.Cancel();
                 }
                 return Task.CompletedTask;
             }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("SubscribeWait async topic to get byte messages")]
        public async Task SubscribeWaitAsync2()
        {
            const string deadletterQueue = "error.SubscribeWaitAsync1";

            byte[] message = Encoding.Default.GetBytes("hello");
            await _eventBus.PublishAsync("SubscribeWaitAsync1", new byte[][] { message });

            CancellationTokenSource sub = new();

            var completion = new CountdownEvent(1);
            await _eventBus.SubscribeWait("SubscribeWaitAsync1", "SubscribeWaitAsync1", "SubscribeWaitAsync1", (byte[] t) =>
             {
                 Assert.AreEqual(message.Length, t.Length);
                 completion.Signal();

                 if (completion.CurrentCount == 0)
                 {
                     sub.Cancel();
                 }
                 return Task.CompletedTask;
             }, sub.Token, deadletterQueue);
            completion.Wait();
            sub.Cancel();
        }

        [TestMethod("SubscribeWait async topic to get object messages")]
        public async Task SubscribeWaitAsync3()
        {
            const string deadletterQueue = "error.SubscribeWaitAsync3";

            var message = new ServiceEvent<TwilioRecordingReady>();
            await _eventBus.PublishAsync("SubscribeWaitAsync3", message);

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(1);
            await _eventBus.SubscribeWait("SubscribeWaitAsync3", "SubscribeWaitAsync3", "SubscribeWaitAsync3", (object? t) =>
            {
                switch (t)
                {
                    case ServiceEvent<TwilioRecordingReady> _:
                        completion.Signal();
                        break;
                    default:
                        Assert.Fail();
                        break;
                }

                if (completion.CurrentCount == 0)
                {
                    sub.Cancel();
                }
                return Task.CompletedTask;
            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();
        }


        [TestMethod("SubscribeWait async. The sequence of communications is tracked using this test")]
        public async Task SubscribeWaitAsync4()
        {
            const string deadletterQueue = "error.SubscribeWaitAsync4";
            const string callingMethodName = "SubscribeWaitAsync4";
            var listPost = new Stack<int>();
            var listReceive = new Stack<int>();
            int messageCount = 100;
            for (int i = 0; i < messageCount; i++)
            {
                listPost.Push(i);
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello" });
            }

            CancellationTokenSource sub = new();
            var completion = new CountdownEvent(100);
            await _eventBus.SubscribeWait(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                listReceive.Push(listPost.Pop());
                completion.Signal();
                if (completion.CurrentCount == 0)
                {
                    sub.Cancel();
                }
                return Task.CompletedTask;
            }, sub.Token, deadletterQueue);

            completion.Wait();
            sub.Cancel();

            Assert.AreEqual(0, listPost.Count);
            Assert.AreEqual(messageCount, listReceive.Count);
            Assert.AreEqual(0, completion.CurrentCount);
        }

        [TestMethod("Send message to dead letter queue: SubscribeWait async")]
        public async Task PubSubWaitDLQ()
        {
            const string callingMethodName = "PubSubWaitDLQ";
            const string deadletterQueue = "error.PubSubWaitDLQ";

            for (int i = 0; i < 10; i++)
            {
                await _eventBus.PublishAsync(callingMethodName, new string[] { "hello-" + i });
            }

            CancellationTokenSource sub1 = new();
            var counter1 = new CountdownEvent(10);
            await _eventBus.SubscribeWait(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter1.Signal();
                if (counter1.CurrentCount == 0)
                {
                    sub1.Cancel();
                }
                throw new EventProcessingException();
            }, sub1.Token, deadletterQueue);

            counter1.Wait();
            sub1.Cancel();
            Assert.AreEqual(0, counter1.CurrentCount);

            //It take some time for aknowledged message updated in redis
            await Task.Delay(1000);
            CancellationTokenSource sub2 = new(1000);
            var counter2 = new CountdownEvent(1);
            await _eventBus.SubscribeWait(callingMethodName, callingMethodName, callingMethodName, (string t) =>
            {
                counter2.Signal();
                if (counter2.CurrentCount == 0)
                {
                    sub2.Cancel();
                }
                return Task.CompletedTask;
            }, sub2.Token, deadletterQueue);

            counter2.Wait(1000);
            sub2.Cancel();

            Assert.AreEqual(1, counter2.CurrentCount);

            CancellationTokenSource sub3 = new();
            var counter3 = new CountdownEvent(10);
            await _eventBus.SubscribeWait(deadletterQueue, callingMethodName, callingMethodName, (string t) =>
            {
                counter3.Signal();
                if (counter3.CurrentCount == 0)
                {
                    sub3.Cancel();
                }
                return Task.CompletedTask;
            }, sub3.Token);

            counter3.Wait();
            sub3.Cancel();

            Assert.AreEqual(0, counter3.CurrentCount);
        }
    }
}