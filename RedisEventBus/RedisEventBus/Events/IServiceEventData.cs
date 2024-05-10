namespace RedisEventBus.Vinscom.RedisEventBus.Events
{
    public interface IServiceEventData
    {
        string SpecVersion { get; }

        /// <remarks>
        /// Time at which event was created.
        /// </remarks> 
        DateTime CreateOn { get; }
    }
}