namespace RedisEventBus.Events
{
    public class ServiceEvent<T> where T : IServiceEventData, new()
    {
        private const string SPEC_VERSION = "1.0";

        /// <remarks>
        /// Message Id: Used to track event or to avoid duplicate event 
        /// </remarks>
        public string MId { get; set; } = string.Empty;
        public string Type { get; set; } = typeof(T).ToString();
        public string SpecVersion { get; } = SPEC_VERSION;

        /// <remarks>
        /// Source of event published from.
        /// </remarks> 
        public string Source { get; set; } = string.Empty;

        /// <remarks>
        /// Customer/User Id event associated with.
        /// </remarks>        
        public string CustomerId { get; set; } = string.Empty;

        /// <remarks>
        /// Id of Subject(e.g TranscriptId, OrderId, DocumentId etc.) to which event is associated.
        /// </remarks>       
        public string SubjectId { get; set; } = string.Empty;

        /// <remarks>
        /// Time at which event was published.
        /// </remarks> 
        public DateTime TimeStamp { get; set; } = DateTime.UtcNow;

        /// <remarks>
        /// Used to keep data related to processing event
        /// </remarks>    
        public T Properties { get; set; } = new();
    }
}