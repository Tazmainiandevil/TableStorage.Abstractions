using Azure;
using Azure.Data.Tables;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Validators;

namespace TableStorage.Abstractions.Store
{
    public class TableStoreBase
    {
        /// <summary>
        /// The max size for a single partition to be added to Table Storage
        /// </summary>
        protected const int MaxPartitionSize = 100;

        /// <summary>
        /// The cloud table
        /// </summary>
        protected readonly TableClient CloudTable;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        protected TableStoreBase(string tableName, string storageConnectionString) : this(tableName, storageConnectionString, new TableStorageOptions())
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        /// <param name="options">Table storage options</param>
        protected TableStoreBase(string tableName, string storageConnectionString, TableStorageOptions options)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
            }

            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new ArgumentException("Table connection string cannot be null or empty", nameof(storageConnectionString));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options), "Table storage options cannot be null");
            }

            var validator = new TableStorageOptionsValidator();
            validator.ValidateAndThrow(options);

            OptimisePerformance(storageConnectionString, options);
            var cloudTableClient = CreateTableClient(storageConnectionString, options.Retries, options.RetryWaitTimeInSeconds);

            CloudTable = cloudTableClient.GetTableClient(tableName);

            if (options.EnsureTableExists)
            {
                //if (!TableExists())
                //{
                CreateTable();
                //}
            }
        }

        /// <summary>
        /// Settings to improve performance
        /// </summary>
        private static void OptimisePerformance(string storageConnectionString, TableStorageOptions options)
        {
            // var account = CloudStorageAccount.Parse(storageConnectionString);
            //var tableServicePoint = ServicePointManager.FindServicePoint(account.TableEndpoint);
            //tableServicePoint.UseNagleAlgorithm = options.UseNagleAlgorithm;
            //tableServicePoint.Expect100Continue = options.Expect100Continue;
            //tableServicePoint.ConnectionLimit = options.ConnectionLimit;
        }

        /// <summary>
        /// Create the table client
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        /// <param name="retries">Number of retries</param>
        /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
        /// <returns>The table client</returns>
        private static TableServiceClient CreateTableClient(string connectionString, int retries, double retryWaitTimeInSeconds)
        {
            var options = new TableClientOptions
            {
                Retry =
                {
                    MaxRetries = retries,
                    Delay = TimeSpan.FromSeconds(retryWaitTimeInSeconds),
                    Mode = Azure.Core.RetryMode.Exponential
                }
            };


            var cloudTableClient = new TableServiceClient(connectionString, options);

            //var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            //var requestOptions = new TableRequestOptions
            //{
            //    RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(retryWaitTimeInSeconds), retries)
            //};

            //var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            //cloudTableClient.DefaultRequestOptions = requestOptions;
            return cloudTableClient;
        }

        /// <summary>
        /// Create the table
        /// </summary>
        public void CreateTable()
        {
            CloudTable.CreateIfNotExists();
        }

        /// <summary>
        /// Create the table
        /// </summary>
        public async Task CreateTableAsync()
        {
            await CloudTable.CreateIfNotExistsAsync().ConfigureAwait(false);
        }

        ///// <summary>
        ///// Does the table exist
        ///// </summary>
        ///// <returns></returns>
        //public bool TableExists(string tableName)
        //{
        //    var queryTableResults = CloudTable.Query(filter: $"TableName eq '{tableName}'");

        //    return CloudTable.Exists();
        //}

        ///// <summary>
        ///// Does the table exist
        ///// </summary>
        ///// <returns></returns>
        //public async Task<bool> TableExistsAsync()
        //{
        //    return await CloudTable.ExistsAsync().ConfigureAwait(false);
        //}

        /// <summary>
        /// Delete the table
        /// </summary>
        public void DeleteTable()
        {
            CloudTable.Delete();
        }

        /// <summary>
        /// Delete the table
        /// </summary>
        public async Task DeleteTableAsync()
        {
            await CloudTable.DeleteAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        public int GetRecordCount()
        {

            var items = CloudTable.Query<TableEntity>(select: new List<string>() { "PartitionKey" });
            return items.Count();

            //TableContinuationToken continuationToken = null;

            //var query = new TableQuery().Select(new List<string> { "PartitionKey" });


            //var recordCount = 0;
            //do
            //{
            //    var items = CloudTable.ExecuteQuerySegmented(query, continuationToken);
            //    continuationToken = items.ContinuationToken;

            //    recordCount += items.Count();
            //} while (continuationToken != null);

            //return recordCount;
        }

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        public async Task<int> GetRecordCountAsync()
        {
            var items = CloudTable.QueryAsync<TableEntity>(select: new List<string>() { "PartitionKey" });
            int count = 0;

            // Iterate the list in order to access individual queried entities.
            await foreach (TableEntity qEntity in items)
            {
                count++;
            }

            return count;

            //TableContinuationToken continuationToken = null;

            //var query = new TableQuery().Select(new List<string> { "PartitionKey" });

            //var recordCount = 0;
            //do
            //{
            //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
            //    continuationToken = items.ContinuationToken;

            //    recordCount += items.Count();
            //} while (continuationToken != null);

            //return recordCount;
        }

        #region Helpers

        /// <summary>
        /// Ensures the partition key is not null.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <exception cref="ArgumentNullException">partitionKey</exception>
        protected void EnsurePartitionKey(string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey), "PartitionKey cannot be null or empty");
            }
        }

        /// <summary>
        /// Ensures the row key is not null.
        /// </summary>
        /// <param name="rowKey">The row key.</param>
        /// <exception cref="ArgumentNullException">rowKey</exception>
        protected void EnsureRowKey(string rowKey)
        {
            if (string.IsNullOrWhiteSpace(rowKey))
            {
                throw new ArgumentNullException(nameof(rowKey), "RowKey cannot be null or empty");
            }
        }

        protected void EnsureRecord<T>(T record)
        {
            if (record == null)
            {
                throw new ArgumentNullException(nameof(record), "Record cannot be null");
            }
        }


        /// <summary>
        /// Builds the get by partition query.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <returns>The table query</returns>
        //private static TableQuery<T> BuildGetByPartitionQuery(string partitionKey)
        protected Pageable<T> BuildGetByPartitionQuery<T>(string partitionKey) where T : class, ITableEntity, new()
        {
            Pageable<T> queryResults = CloudTable.Query<T>(filter: $"PartitionKey eq '{partitionKey}'");
            return queryResults;
        }

        /// <summary>
        /// Build the row key table query
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The table query</returns>
        protected Pageable<T> BuildGetByRowKeyQuery<T>(string rowKey) where T : class, ITableEntity, new()
        {
            Pageable<T> queryResults = CloudTable.Query<T>(filter: $"RowKey eq '{rowKey}'");
            return queryResults;
        }

        #endregion Helpers
    }
}