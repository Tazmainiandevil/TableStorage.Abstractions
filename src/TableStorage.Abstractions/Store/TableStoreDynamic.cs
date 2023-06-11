using Azure;
using Azure.Core;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Useful.Extensions;

namespace TableStorage.Abstractions.Store
{
    public class TableStoreDynamic : TableStoreBase, ITableStoreDynamic
    {
        /// <inheritdoc/>
        public TableStoreDynamic(string tableName, string storageConnectionString)
            : base(tableName, storageConnectionString, new TableStorageOptions())
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string tableName, string storageConnectionString, TableStorageOptions options)
            : base(tableName, storageConnectionString, options)
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string accountName, string tableName, TokenCredential tokenCredential)
            : base(accountName, tableName, tokenCredential, new TableStorageOptions())
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string accountName, string tableName, TokenCredential tokenCredential, TableStorageOptions options)
            : base(accountName, tableName, tokenCredential, options)
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string accountName, string tableName, AzureSasCredential sasCredential)
            : base(accountName, tableName, sasCredential, new TableStorageOptions())
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string accountName, string tableName, AzureSasCredential sasCredential, TableStorageOptions options)
            : base(accountName, tableName, sasCredential, options)
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential)
            : base(accountName, tableName, sharedKeyCredential, new TableStorageOptions())
        {
        }

        /// <inheritdoc/>
        public TableStoreDynamic(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential, TableStorageOptions options)
            : base(accountName, tableName, sharedKeyCredential, options)
        {
        }

        /// <inheritdoc/>
        public void Insert<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            CloudTable.AddEntity(record);
        }

        /// <inheritdoc/>
        public Task InsertAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);
            return CloudTable.AddEntityAsync(record, cancellationToken);
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public async Task InsertAsync<T>(IEnumerable<T> records, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
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
                    await CloudTable.AddEntityAsync(qEntity, cancellationToken);
                }
            }
        }

        /// <inheritdoc/>
        public void InsertOrReplace<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            CloudTable.UpsertEntity(record);
        }

        /// <inheritdoc/>
        public Task InsertOrReplaceAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            return CloudTable.UpsertEntityAsync(record, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public void Update<T>(T record) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            CloudTable.UpdateEntity(record, record.ETag);
        }

        /// <inheritdoc/>
        public Task UpdateAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            EnsureRecord(record);

            return CloudTable.UpdateEntityAsync(record, record.ETag, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public void Delete<T>(T record) where T : class, ITableEntity
        {
            EnsureRecord(record);

            CloudTable.DeleteEntity(record.PartitionKey, record.RowKey);
        }

        /// <inheritdoc/>
        public Task DeleteAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity
        {
            EnsureRecord(record);

            return CloudTable.DeleteEntityAsync(record.PartitionKey, record.RowKey, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public T GetRecord<T>(string partitionKey, string rowKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            return CloudTable.GetEntity<T>(partitionKey, rowKey);
        }

        /// <inheritdoc/>
        public async Task<T> GetRecordAsync<T>(string partitionKey, string rowKey, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            return await CloudTable.GetEntityAsync<T>(partitionKey, rowKey, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public IEnumerable<TableEntity> GetAllRecords()
        {
            var query = CloudTable.Query<TableEntity>();
            return query;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<TableEntity>> GetAllRecordsAsync(CancellationToken cancellationToken = default)
        {
            var queryResults = CloudTable.QueryAsync<TableEntity>(cancellationToken: cancellationToken);

            return await queryResults.ToListAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public IEnumerable<T> GetByPartitionKey<T>(string partitionKey) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            var query = BuildGetByPartitionQuery<T>(partitionKey);
            return query;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<T>> GetByPartitionKeyAsync<T>(string partitionKey, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            EnsurePartitionKey(partitionKey);

            var queryResults = CloudTable.QueryAsync<T>(filter: $"PartitionKey eq '{partitionKey}'", cancellationToken: cancellationToken);

            return await queryResults.ToListAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public IEnumerable<T> GetByRowKey<T>(string rowKey) where T : class, ITableEntity, new()
        {
            EnsureRowKey(rowKey);

            return BuildGetByRowKeyQuery<T>(rowKey);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<T>> GetByRowKeyAsync<T>(string rowKey, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            EnsureRowKey(rowKey);
            var queryResults = CloudTable.QueryAsync<T>(filter: $"RowKey eq '{rowKey}'", cancellationToken: cancellationToken);

            return await queryResults.ToListAsync(cancellationToken);
        }
    }
}