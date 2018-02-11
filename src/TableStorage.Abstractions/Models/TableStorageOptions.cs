namespace TableStorage.Abstractions.Models
{
    public class TableStorageOptions
    {
        /// <summary>
        /// Nagle's algorithm is a performance optimization for TCP/IP based networks but it has a negative impact on performance of requests 
        /// when using Azure Storage services.
        /// </summary>
        public bool UseNagleAlgorithm { get; set; } = false;

        /// <summary>
        /// Setting the Expect100Continue property to false configures the client to not wait for a 100-Continue response from the server before transmitting data.
        /// Waiting for 100-Continue is an optimization to avoid sending larger payloads when the server rejects the request. 
        /// That optimization isn't necessary for Azure Storage operations and disabling it may result in faster requests.
        /// </summary>
        public bool Expect100Continue { get; set; } = false;

        /// <summary>
        /// The .NET Framework is configured to only allow 2 simultaneous connections to the same resource by default. A higher connection limit allows more parallel
        /// requests and therefore results in a higher network throughput. Setting the connection limit too high bypasses the built in connection reuse mechanism which 
        /// may result in a sub-optimal resource usage.
        /// The optimal value depends on the physical properties of the host machine and the endpoint's expected workload. 
        /// The ideal number is lower than the average amount of parallel storage operations. It is recommended to start with a value of 10 and adjusting the value based on 
        /// the observed performance impact.
        /// See http://tk.azurewebsites.net/2012/12/10/greatly-increase-the-performance-of-azure-storage-cloudblobclient/
        /// and https://docs.particular.net/persistence/azure-storage/performance-tuning
        /// and https://github.com/giometrix/TableStorage.Abstractions.Trie#single-index for details and benchmarks.
        /// </summary>
        public int ConnectionLimit { get; set; } = 10;

        /// <summary>
        /// Number of retries
        /// </summary>
        public int Retries { get; set; } = 3;

        /// <summary>
        /// Wait time between retries in seconds
        /// </summary>
        public double RetryWaitTimeInSeconds { get; set; } = 1;

        /// <summary>
        /// When set to true the Azure table will be created if it does not exist. 
        /// Set to false to improve performance in cases where this class is instantiated frequently.
        /// </summary>
        public bool EnsureTableExists { get; set; } = true;
    }
}