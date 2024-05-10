using RedisEventBus.RedisEventBus.Events;

namespace RedisEventBus.RedisEventBus.Events.SampleEvent
{
    public class SampleEvent : ServiceEventData
    {
        public string Action { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }
}
