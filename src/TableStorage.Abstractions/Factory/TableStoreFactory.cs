using System;
using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Factory
{
    public class TableStoreFactory : ITableStoreFactory
    {
        private const int DefaultMaxNumberOfConnections = 20;

        /// <summary>
        /// Sets the maximum number connections.  The default is 2, which is not a good number for high throughput in many cases.
        /// See http://tk.azurewebsites.net/2012/12/10/greatly-increase-the-performance-of-azure-storage-cloudblobclient/
        /// and https://github.com/giometrix/TableStorage.Abstractions.Trie#single-index for details and benchmarks.
        /// </summary>
        /// <param name="storageConnectionString">Azure Storage connection string.</param>
        /// <param name="maxNumberOfConnections">The maximum number of connections.</param>
        /// <exception cref="ArgumentException">maxNumberOfConnections</exception>
        public static void SetMaxNumberConnections(string storageConnectionString, int maxNumberOfConnections)
        {
            if(String.IsNullOrWhiteSpace(storageConnectionString))
                throw new ArgumentNullException(nameof(storageConnectionString));

            if(maxNumberOfConnections < 1)
                throw new ArgumentException(nameof(maxNumberOfConnections));

            var account = CloudStorageAccount.Parse(storageConnectionString);
            var tableServicePoint = ServicePointManager.FindServicePoint(account.TableEndpoint);
            if (tableServicePoint.ConnectionLimit != maxNumberOfConnections)
                tableServicePoint.ConnectionLimit = maxNumberOfConnections;
        }
        public ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString) where T : class,ITableEntity, new()
        {
            SetMaxNumberConnections(storageConnectionString, DefaultMaxNumberOfConnections);

            return new TableStore<T>(tableName, storageConnectionString);
        }

        public ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString, int retries, double retryWaitTimeInSeconds, int maxNumberOfConnections, bool ensureTableExists = true) where T : class, ITableEntity, new()
        {
            SetMaxNumberConnections(storageConnectionString, maxNumberOfConnections);

            return new TableStore<T>(tableName, storageConnectionString, retries, retryWaitTimeInSeconds, ensureTableExists);
        }
    }
}