using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TableStorage.Abstractions
{
    /// <summary>
    /// Table store repository
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TableStore<T> : ITableStore<T> where T : TableEntity, new()
    {
        /// <summary>
        /// The cloud table
        /// </summary>
        private readonly CloudTable _cloudTable;

        /// <summary>
        /// The default retries
        /// </summary>
        private const int DefaultRetries = 3;

        /// <summary>
        /// The default retry in seconds
        /// </summary>
        private const double DefaultRetryTimeInSeconds = 1;

        #region Construction

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        public TableStore(string tableName, string storageConnectionString)
        : this(tableName, storageConnectionString, DefaultRetries, DefaultRetryTimeInSeconds) { }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        /// <param name="retries">Number of retries</param>
        /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
        public TableStore(string tableName, string storageConnectionString, int retries, double retryWaitTimeInSeconds)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException(nameof(tableName));
            }

            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }

            var cloudTableClient = CreateTableClient(storageConnectionString, retries, retryWaitTimeInSeconds);

            _cloudTable = cloudTableClient.GetTableReference(tableName);
            CreateTable();
        }

        /// <summary>
        /// Create the table client
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        /// <param name="retries">Number of retries</param>
        /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
        /// <returns>The table client</returns>
        private static CloudTableClient CreateTableClient(string connectionString, int retries, double retryWaitTimeInSeconds)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var requestOptions = new TableRequestOptions
            {
                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(retryWaitTimeInSeconds), retries)
            };

            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            cloudTableClient.DefaultRequestOptions = requestOptions;
            return cloudTableClient;
        }

        #endregion Construction

        #region Synchronous Methods

        /// <summary>
        /// Create the table
        /// </summary>
        public void CreateTable()
        {
            _cloudTable.CreateIfNotExists();
        }

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns></returns>
        public bool TableExists()
        {
            return _cloudTable.Exists();
        }


        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public void Insert(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            var operation = TableOperation.Insert(record);
            _cloudTable.Execute(operation);
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public void Insert(IEnumerable<T> records)
        {
            if (records == null)
            {
                throw new ArgumentNullException(nameof(records));
            }

            var partitionSeparation = records.GroupBy(x => x.PartitionKey)
                .OrderBy(g => g.Key)
                .Select(g => g.ToList());

            foreach (var entry in partitionSeparation)
            {
                var operation = new TableBatchOperation();
                entry.ForEach(operation.Insert);

                if (operation.Any())
                {
                    _cloudTable.ExecuteBatch(operation);
                }
            }
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public void Update(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }
            var operation = TableOperation.Merge(record);

            _cloudTable.Execute(operation);
        }

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void Delete(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            var operation = TableOperation.Delete(record);
            _cloudTable.Execute(operation);
        }
       
        /// <summary>
        /// Delete the table
        /// </summary>
        public void DeleteTable()
        {
            _cloudTable.DeleteIfExists();
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public T GetRecord(string partitionKey, string rowKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            if (string.IsNullOrWhiteSpace(rowKey))
            {
                throw new ArgumentNullException(nameof(rowKey));
            }

            // Create a retrieve operation that takes a customer record.
            var retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            // Execute the operation.
            var retrievedResult = _cloudTable.Execute(retrieveOperation);

            return retrievedResult.Result as T;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByPartitionKey(string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            var query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            var items = _cloudTable.ExecuteQuery(query).AsEnumerable();

            return items;
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByRowKey(string rowKey)
        {
            if (string.IsNullOrWhiteSpace(rowKey))
            {
                throw new ArgumentNullException(nameof(rowKey));
            }

            var query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            var items = _cloudTable.ExecuteQuery(query).AsEnumerable();

            return items;
        }

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public IEnumerable<T> GetAllRecords()
        {
            TableContinuationToken continuationToken = null;

            var query = new TableQuery<T>();

            var alllItems = new List<T>();
            do
            {
                var items = _cloudTable.ExecuteQuerySegmented(query, continuationToken);
                continuationToken = items.ContinuationToken;
                alllItems.AddRange(items);
            } while (continuationToken != null);

            return alllItems;
        }

        #endregion Synchronous Methods

        #region Asynchronous Methods

        /// <summary>
        /// Create the table
        /// </summary>
        public async Task CreateTableAsync()
        {
            await _cloudTable.CreateIfNotExistsAsync();
        }

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns></returns>
        public async Task<bool> TableExistsAsync()
        {
            return await _cloudTable.ExistsAsync();
        }

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public async Task InsertAsync(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            var operation = TableOperation.Insert(record);

            await _cloudTable.ExecuteAsync(operation).ConfigureAwait(false);
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public async Task InsertAsync(IEnumerable<T> records)
        {
            if (records == null)
            {
                throw new ArgumentNullException(nameof(records));
            }
          
            var partitionSeparation = records.GroupBy(x => x.PartitionKey)
           .OrderBy(g => g.Key)
           .Select(g => g.ToList());

            foreach (var entry in partitionSeparation)
            {
                var operation = new TableBatchOperation();
                entry.ForEach(operation.Insert);

                if (operation.Any())
                {
                    await _cloudTable.ExecuteBatchAsync(operation);
                }
            }
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public async Task UpdateAsync(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            var operation = TableOperation.Merge(record);

            await _cloudTable.ExecuteAsync(operation).ConfigureAwait(false);
        }

        /// <summary>
        /// Update an entry
        /// </summary>
        /// <param name="record">The record to update</param>
        public async Task DeleteAsync(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record));
            }

            var operation = TableOperation.Delete(record);

            await _cloudTable.ExecuteAsync(operation).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete the table
        /// </summary>
        public async Task DeleteTableAsync()
        {
            await _cloudTable.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public async Task<T> GetRecordAsync(string partitionKey, string rowKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            if (string.IsNullOrWhiteSpace(rowKey))
            {
                throw new ArgumentNullException(nameof(rowKey));
            }

            // Create a retrieve operation that takes a customer record.
            var retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            // Execute the operation.
            var retrievedResult = await _cloudTable.ExecuteAsync(retrieveOperation);

            return retrievedResult.Result as T;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByPartitionKeyAsync(string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            TableContinuationToken continuationToken = null;

            var query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            var alllItems = new List<T>();
            do
            {
                var items = await _cloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                continuationToken = items.ContinuationToken;
                alllItems.AddRange(items);
            } while (continuationToken != null);

            return alllItems;
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByRowKeyAsync(string rowKey)
        {
            if (string.IsNullOrWhiteSpace(rowKey))
            {
                throw new ArgumentNullException(nameof(rowKey));
            }

            TableContinuationToken continuationToken = null;

            var query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            var alllItems = new List<T>();
            do
            {
                var items = await _cloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                continuationToken = items.ContinuationToken;
                alllItems.AddRange(items);
            } while (continuationToken != null);

            return alllItems;
        }

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public async Task<IEnumerable<T>> GetAllRecordsAsync()
        {
            TableContinuationToken continuationToken = null;

            var query = new TableQuery<T>();

            var alllItems = new List<T>();
            do
            {
                var items = await _cloudTable.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = items.ContinuationToken;
                alllItems.AddRange(items);
            } while (continuationToken != null);

            return alllItems;
        }

        #endregion Asynchronous Methods
    }
}