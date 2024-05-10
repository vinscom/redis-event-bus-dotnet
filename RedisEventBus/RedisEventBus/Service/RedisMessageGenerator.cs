using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedisEventBus.Vinscom.RedisEventBus.Service
{
    internal class RedisMessageGenerator
    {
        private readonly IDatabase _db;
        private readonly string _groupName;
        private readonly string _consumer;
        private readonly string _topic;
        private readonly int _msgCount;
        private RedisValue _position;
        private readonly bool _checkPending;
        private readonly Queue<StreamEntry> _eventQueue;
        private readonly ILogger? _logger;
        private readonly Func<string> _logInfo;
        private readonly CancellationToken _cts;

        public RedisMessageGenerator(IDatabase db, string topic, string groupName, string consumer, bool checkPending, CancellationToken cts, int msgCount = 5, ILogger? logger = null)
        {
            _db = db;
            _topic = topic;
            _consumer = consumer;
            _groupName = groupName;
            _msgCount = msgCount;
            _logger = logger;
            _checkPending = checkPending;

            if (_checkPending)
            {
                _position = "0-0";
            }
            else
            {
                _position = ">";
            }

            _cts = cts;
            _eventQueue = new Queue<StreamEntry>();

            _logInfo = () => $"[{Environment.CurrentManagedThreadId}:{_topic}:{_groupName}:{_consumer}:{GetHashCode()}:{_position}]";
        }

        public bool HasMore()
        {
            if (_cts.IsCancellationRequested)
            {
                _logger?.LogTrace("{info}\t[Fn:HasMore]\t[CTS-{cts} Cancelled]", _logInfo(), _cts.GetHashCode());
                return false;
            }

            if (_eventQueue.Count == 0)
            {
                _logger?.LogTrace("{info}\t[Fn:HasMore]\t[No item in queue]", _logInfo());
                var messages = _db.StreamReadGroup(_topic, _groupName, _consumer, _position, _msgCount);

                foreach (var message in messages)
                {
                    _eventQueue.Enqueue(message);
                }
            }

            if (_eventQueue.Count > 0)
            {
                _logger?.LogTrace("{info}\t[Fn:HasMore]\t[Item found in Queue:{count}]", _logInfo(), _eventQueue.Count);
                return true;
            }

            _logger?.LogTrace("{info}\t[Fn:HasMore]\t[No item in queue after reading again]", _logInfo());
            return false;
        }

        public StreamEntry? Next()
        {

            if (_cts.IsCancellationRequested)
            {
                _logger?.LogTrace("{info}\t[Fn:Next]\t[Cancellation Called{cts}, return null]", _logInfo(), _cts.GetHashCode());
                return null;
            }

            if (_eventQueue.Count > 0)
            {
                _logger?.LogTrace("{info}\t[Fn:Next]\t[Item Found, Returing 1 item]", _logInfo());
                var msg = _eventQueue.Dequeue();

                if (_checkPending)
                {
                    _position = msg.Id;
                }

                return msg;
            }

            _logger?.LogTrace("{info}\t[Fn:Next]\t[No item found, return null]", _logInfo());
            return null;
        }
    }

}