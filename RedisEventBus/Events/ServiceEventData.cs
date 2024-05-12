namespace RedisEventBus.Events
{
    public class ServiceEventData : IServiceEventData
    {
        private const string SPEC_VERSION = "1.0";
        public virtual string SpecVersion { get; } = SPEC_VERSION;

        public virtual DateTime CreateOn { get; } = DateTime.UtcNow;
    }
}
