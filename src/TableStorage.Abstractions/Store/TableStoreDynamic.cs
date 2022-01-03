using Azure.Data.Tables;
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
        public void Insert<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            CloudTable.AddEntity(record);
        }

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public Task InsertAsync<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);
            return CloudTable.AddEntityAsync(record);
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public void Insert<T>(IEnumerable<T> records) where T : class, ITableEntity, new()
        {
            if (records == null)
            {
                throw new ArgumentNullException(nameof(records), "Records cannot be null");
            }

            var partitionKeySeparation = records.GroupBy(x => x.PartitionKey)
                .OrderBy(g => g.Key)
                .Select(g => g.AsEnumerable()).SelectMany(entry => entry.Partition(MaxPartitionSize)).ToList();

            foreach (var entry in partitionKeySeparation)
            {
                entry.ToList().ForEach(x => CloudTable.AddEntity(x));
            }
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public async Task InsertAsync<T>(IEnumerable<T> records) where T : class, ITableEntity, new()
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
                await foreach (T qEntity in entry.ToAsyncEnumerable())
                {
                    await CloudTable.AddEntityAsync(qEntity);
                }
            }
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        public void InsertOrReplace<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            CloudTable.UpsertEntity(record);
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        public Task InsertOrReplaceAsync<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            return CloudTable.UpsertEntityAsync(record);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public void Update<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            CloudTable.UpdateEntity(record, record.ETag, TableUpdateMode.Merge);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public Task UpdateAsync<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            return CloudTable.UpdateEntityAsync(record, record.ETag);
        }

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void Delete<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            CloudTable.DeleteEntity(record.PartitionKey, record.RowKey);
        }

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public Task DeleteAsync<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            return CloudTable.DeleteEntityAsync(record.PartitionKey, record.RowKey);
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public T GetRecord<T>(string partitionKey, string rowKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            return CloudTable.GetEntity<T>(partitionKey, rowKey);
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public async Task<T> GetRecordAsync<T>(string partitionKey, string rowKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            return await CloudTable.GetEntityAsync<T>(partitionKey, rowKey);
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByPartitionKey<T>(string partitionKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            var query = BuildGetByPartitionQuery<T>(partitionKey);
            return query;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByPartitionKeyAsync<T>(string partitionKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            var queryResults = CloudTable.QueryAsync<T>(filter: $"PartitionKey eq '{partitionKey}'");

            return await queryResults.ToListAsync();
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByRowKey<T>(string rowKey) where T : class, ITableEntity, new()
        {
            EnsureRowKey(rowKey);

            return BuildGetByRowKeyQuery<T>(rowKey);
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByRowKeyAsync<T>(string rowKey) where T : class, ITableEntity, new()
        {
            EnsureRowKey(rowKey);
            var queryResults = CloudTable.QueryAsync<T>(filter: $"RowKey eq '{rowKey}'");

            return await queryResults.ToListAsync();
        }
    }
}