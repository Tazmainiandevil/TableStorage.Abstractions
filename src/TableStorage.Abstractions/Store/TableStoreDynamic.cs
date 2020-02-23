using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Useful.Extensions;

namespace TableStorage.Abstractions.Store
{
    public class TableStoreDynamic : TableStoreBase, ITableStoreDynamic
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        public TableStoreDynamic(string tableName, string storageConnectionString)
            : base(tableName, storageConnectionString, new TableStorageOptions())
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        /// <param name="options">Table storage options</param>
        public TableStoreDynamic(string tableName, string storageConnectionString, TableStorageOptions options)
            : base(tableName, storageConnectionString, options)
        {
        }

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public void Insert<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.Insert(record);
            CloudTable.Execute(operation);
        }

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public Task InsertAsync<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.Insert(record);

            return CloudTable.ExecuteAsync(operation);
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public void Insert<T>(IEnumerable<T> records) where T : class, ITableEntity
        {
            if (records == null)
            {
                throw new ArgumentNullException(nameof(records));
            }

            var partitionKeySeparation = records.GroupBy(x => x.PartitionKey)
                .OrderBy(g => g.Key)
                .Select(g => g.AsEnumerable()).SelectMany(entry => entry.Partition(MaxPartitionSize)).ToList();

            foreach (var entry in partitionKeySeparation)
            {
                var operation = new TableBatchOperation();
                entry.ToList().ForEach(operation.Insert);

                if (operation.Any())
                {
                    CloudTable.ExecuteBatch(operation);
                }
            }
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public async Task InsertAsync<T>(IEnumerable<T> records) where T : class, ITableEntity
        {
            if (records == null)
            {
                throw new ArgumentNullException(nameof(records));
            }

            var partitionKeySeparation = records.GroupBy(x => x.PartitionKey)
                .OrderBy(g => g.Key)
                .Select(g => g.AsEnumerable()).SelectMany(entry => entry.Partition(MaxPartitionSize)).ToList();

            foreach (var entry in partitionKeySeparation)
            {
                var operation = new TableBatchOperation();
                entry.ToList().ForEach(operation.Insert);

                if (operation.Any())
                {
                    await CloudTable.ExecuteBatchAsync(operation).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        public void InsertOrReplace<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.InsertOrReplace(record);
            CloudTable.Execute(operation);
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        public Task InsertOrReplaceAsync<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.InsertOrReplace(record);

            return CloudTable.ExecuteAsync(operation);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public void Update<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.Merge(record);
            CloudTable.Execute(operation);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public Task UpdateAsync<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.Merge(record);

            return CloudTable.ExecuteAsync(operation);
        }

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void Delete<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.Delete(record);
            CloudTable.Execute(operation);
        }

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public Task DeleteAsync<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            var operation = TableOperation.Delete(record);

            return CloudTable.ExecuteAsync(operation);
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public T GetRecord<T>(string partitionKey, string rowKey) where T : class, ITableEntity
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            // Create a retrieve operation that takes a customer record.
            var retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            // Execute the operation.
            var retrievedResult = CloudTable.Execute(retrieveOperation);
            return retrievedResult.Result as T;
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public async Task<T> GetRecordAsync<T>(string partitionKey, string rowKey) where T : class, ITableEntity
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            // Create a retrieve operation that takes a customer record.
            var retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            // Execute the operation.
            var retrievedResult = await CloudTable.ExecuteAsync(retrieveOperation).ConfigureAwait(false);

            return retrievedResult.Result as T;
        }

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public IEnumerable<DynamicTableEntity> GetAllRecords()
        {
            var query = new TableQuery<DynamicTableEntity>();

            var token = new TableContinuationToken();
            var segment = CloudTable.ExecuteQuerySegmented(query, token);
            while (token != null)
            {
                foreach (var result in segment)
                {
                    yield return result;
                }
                token = segment.ContinuationToken;
                segment = CloudTable.ExecuteQuerySegmented(query, token);
            }
        }

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public async Task<IEnumerable<DynamicTableEntity>> GetAllRecordsAsync()
        {
            TableContinuationToken continuationToken = null;

            var query = new TableQuery<DynamicTableEntity>();

            var allItems = new List<DynamicTableEntity>();
            do
            {
                var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                continuationToken = items.ContinuationToken;
                allItems.AddRange(items);
            } while (continuationToken != null);

            return allItems;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByPartitionKey<T>(string partitionKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            TableContinuationToken continuationToken = null;

            var query = BuildGetByPartitionQuery(partitionKey);

            var allItems = new List<T>();
            do
            {
                var items = ExecuteQuerySegment<T>(query, continuationToken);
                continuationToken = items.ContinuationToken;
                allItems.AddRange(items);
            } while (continuationToken != null);

            return allItems;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByPartitionKeyAsync<T>(string partitionKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            TableContinuationToken continuationToken = null;

            var query = BuildGetByPartitionQuery(partitionKey);

            var allItems = new List<T>();
            do
            {
                var items = await CloudTable.ExecuteQuerySegmentedAsync(query, CreateEntityResolver<T>(), continuationToken).ConfigureAwait(false);
                continuationToken = items.ContinuationToken;
                allItems.AddRange(items);
            } while (continuationToken != null);

            return allItems;
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByRowKey<T>(string rowKey) where T : class, ITableEntity, new()
        {
            EnsureRowKey(rowKey);

            TableContinuationToken continuationToken = null;

            var query = BuildGetByRowKeyQuery(rowKey);

            var allItems = new List<T>();
            do
            {
                var items = ExecuteQuerySegment<T>(query, continuationToken);
                continuationToken = items.ContinuationToken;
                allItems.AddRange(items);
            } while (continuationToken != null);

            return allItems;
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByRowKeyAsync<T>(string rowKey) where T : class, ITableEntity, new()
        {
            EnsureRowKey(rowKey);

            TableContinuationToken continuationToken = null;

            var query = BuildGetByRowKeyQuery(rowKey);

            var allItems = new List<T>();
            do
            {
                var items = await CloudTable.ExecuteQuerySegmentedAsync(query, CreateEntityResolver<T>(), continuationToken).ConfigureAwait(false);
                continuationToken = items.ContinuationToken;
                allItems.AddRange(items);
            } while (continuationToken != null);

            return allItems;
        }

        #region Helpers

        /// <summary>
        /// Create the entity resolver for type T
        /// </summary>
        /// <returns>The entity resolver</returns>
        private static EntityResolver<T> CreateEntityResolver<T>() where T : class, ITableEntity, new()
        {
            return (pk, rk, ts, props, etag) =>
            {
                var resolvedEntity = new T { PartitionKey = pk, RowKey = rk, Timestamp = ts, ETag = etag };
                resolvedEntity.ReadEntity(props, null);
                return resolvedEntity;
            };
        }

        private TableQuerySegment<T> ExecuteQuerySegment<T>(TableQuery<DynamicTableEntity> query, TableContinuationToken continuationToken) where T : class, ITableEntity, new()
        {
            var items = CloudTable.ExecuteQuerySegmented(query, CreateEntityResolver<T>(), continuationToken);
            return items;
        }

        /// <summary>
        /// Build the row key table query
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The table query</returns>
        private static TableQuery<DynamicTableEntity> BuildGetByRowKeyQuery(string rowKey)
        {
            var filter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey);

            var query = new TableQuery<DynamicTableEntity>().Where(filter);

            return query;
        }

        /// <summary>
        /// Builds the get by partition query.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <returns>The table query</returns>
        private static TableQuery<DynamicTableEntity> BuildGetByPartitionQuery(string partitionKey)
        {
            var query = new TableQuery<DynamicTableEntity>().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            return query;
        }

        #endregion Helpers
    }
}