﻿using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Parsers;
using Useful.Extensions;

namespace TableStorage.Abstractions.Store
{
    /// <summary>
    /// Table store repository
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TableStore<T> : TableStoreBase, ITableStore<T> where T : class, ITableEntity, new()
    {
        #region Construction

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        public TableStore(string tableName, string storageConnectionString)
            : base(tableName, storageConnectionString, new TableStorageOptions())
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableName">The table name</param>
        /// <param name="storageConnectionString">The connection string</param>
        /// <param name="options">Table storage options</param>
        public TableStore(string tableName, string storageConnectionString, TableStorageOptions options)
            : base(tableName, storageConnectionString, options)
        {
        }

        #endregion Construction

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void Delete(T record)
        {
            EnsureRecord(record);

            CloudTable.DeleteEntity(record.PartitionKey, record.RowKey);
        }

        /// <summary>
        /// Delete all records in the table
        /// </summary>
        public async Task DeleteAllAsync()
        {
            var records = await GetAllRecordsAsync().ConfigureAwait(false);
            var partitionKeys = records.Select(x => x.PartitionKey).Distinct().ToList();
            foreach (var key in partitionKeys)
            {
                await DeleteByPartitionAsync(key);
            }
        }

        /// <summary>
        /// Delete an entry
        /// </summary>
        /// <param name="record">The record to delete</param>
        public Task DeleteAsync(T record)
        {
            EnsureRecord(record);

            return CloudTable.DeleteEntityAsync(record.PartitionKey, record.RowKey);
        }

        /// <summary>
        /// Delete records by partition key
        /// </summary>
        /// <param name="partitionKey"></param>
        public async Task DeleteByPartitionAsync(string partitionKey)
        {
            var deleteQuery = BuildGetByPartitionQuery<T>(partitionKey);

            var deleteEntitiesBatch = new List<TableTransactionAction>();

            deleteEntitiesBatch.AddRange(deleteQuery.Select(e => new TableTransactionAction(TableTransactionActionType.Delete, e)));

            await CloudTable.SubmitTransactionAsync(deleteEntitiesBatch).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void DeleteUsingWildcardEtag(T record)
        {
            EnsureRecord(record);

            record.ETag = ETag.All;
            Delete(record);
        }

        /// <summary>
        /// Delete a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to delete</param>
        public async Task DeleteUsingWildcardEtagAsync(T record)
        {
            EnsureRecord(record);

            record.ETag = ETag.All;

            await DeleteAsync(record).ConfigureAwait(false);
        }

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public IEnumerable<T> GetAllRecords()
        {
            var query = CloudTable.Query<T>();
            foreach (var result in query)
            {
                yield return result;
            }
        }

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public async Task<IEnumerable<T>> GetAllRecordsAsync()
        {
            var queryResults = CloudTable.QueryAsync<T>();

            return await queryResults.ToListAsync();
        }

        /// <summary>
        /// Get the records via observable
        /// </summary>
        /// <returns>The observable for the results</returns>
        public IObservable<T> GetAllRecordsObservable()
        {
            return Observable.Create<T>(o =>
            {
                foreach (var result in GetAllRecords())
                {
                    o.OnNext(result);
                }
                return Disposable.Empty;
            });
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByPartitionKey(string partitionKey)
        {
            EnsurePartitionKey(partitionKey);

            var query = BuildGetByPartitionQuery<T>(partitionKey);
            return query;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByPartitionKey(string partitionKey, string ago)
        {
            EnsurePartitionKey(partitionKey);

            var query = BuildGetByPartitionAndTimeQuery(partitionKey, ago);
            return query;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByPartitionKeyAsync(string partitionKey)
        {
            EnsurePartitionKey(partitionKey);

            var queryResults = CloudTable.QueryAsync<T>(filter: $"PartitionKey eq '{partitionKey}'");

            return await queryResults.ToListAsync();
        }

        public async Task<IEnumerable<T>> GetByPartitionKeyAsync(string partitionKey, string ago)
        {
            EnsurePartitionKey(partitionKey);

            var utcTime = new DateTimeOffset(TimeStringParser.GetTimeAgo(ago), TimeSpan.Zero);

            var queryResults = CloudTable.QueryAsync<T>(x => x.PartitionKey == partitionKey && x.Timestamp >= utcTime);

            return await queryResults.ToListAsync();
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByRowKey(string rowKey)
        {
            EnsureRowKey(rowKey);

            var query = BuildGetByRowKeyQuery<T>(rowKey);
            return query;
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByRowKey(string rowKey, string ago)
        {
            EnsureRowKey(rowKey);

            var query = BuildGetByRowKeyAndTimeQuery(rowKey, ago);
            return query;
        }

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByRowKeyAsync(string rowKey)
        {
            EnsureRowKey(rowKey);

            var queryResults = CloudTable.QueryAsync<T>(filter: $"RowKey eq '{rowKey}'");

            return await queryResults.ToListAsync();
        }

        public async Task<IEnumerable<T>> GetByRowKeyAsync(string rowKey, string ago)
        {
            EnsureRowKey(rowKey);

            var utcTime = new DateTimeOffset(TimeStringParser.GetTimeAgo(ago), TimeSpan.Zero);

            var queryResults = CloudTable.QueryAsync<T>(x => x.RowKey == rowKey && x.Timestamp >= utcTime);

            return await queryResults.ToListAsync();
        }

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        public T GetRecord(string partitionKey, string rowKey)
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
        public async Task<T> GetRecordAsync(string partitionKey, string rowKey)
        {
            EnsurePartitionKey(partitionKey);

            EnsureRowKey(rowKey);

            return await CloudTable.GetEntityAsync<T>(partitionKey, rowKey);
        }

        /// <summary>
        /// Get the records and filter by a given predicate
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <returns>The records filtered</returns>
        public IEnumerable<T> GetRecordsByFilter(Func<T, bool> filter)
        {
            return GetAllRecords().Where(filter);
        }

        /// <summary>
        /// Get the records and filter by a given predicate and time in the past
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The records filtered</returns>
        public IEnumerable<T> GetRecordsByFilter(Func<T, bool> filter, string ago)
        {
            bool CombineFilter(T x) => filter(x) && x.Timestamp >= TimeStringParser.GetTimeAgo(ago);
            return GetAllRecords().Where(CombineFilter);
        }

        /// <summary>
        /// Get the records and filter by a given predicate
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="start">The start record</param>
        /// <param name="pageSize">The page size</param>
        /// <returns>The records filtered</returns>
        public IEnumerable<T> GetRecordsByFilter(Func<T, bool> filter, int start, int pageSize)
        {
            var items = GetRecordsByFilter(filter);
            return items.Page(start, pageSize);
        }

        /// <summary>
        /// Get the records and filter by a given predicate
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="start">The start record</param>
        /// <param name="pageSize">The page size</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The records filtered</returns>
        public IEnumerable<T> GetRecordsByFilter(Func<T, bool> filter, int start, int pageSize, string ago)
        {
            bool CombineFilter(T x) => filter(x) && x.Timestamp >= TimeStringParser.GetTimeAgo(ago);
            var items = GetRecordsByFilter(CombineFilter);
            return items.Page(start, pageSize);
        }

        /// <summary>
        /// Get the records and filter by a given predicate
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="start">The start record</param>
        /// <param name="pageSize">The page size</param>
        /// <returns>The records filtered</returns>
        public async Task<IEnumerable<T>> GetRecordsByFilterAsync(Func<T, bool> filter, int start, int pageSize)
        {
            var allRecords = GetAllRecords();
            var data = allRecords.Where(filter).Page(start, pageSize);

            return await Task.FromResult(data);
        }

        /// <summary>
        /// Get the records and filter by a given predicate and time in the past
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="start">The start record</param>
        /// <param name="pageSize">The page size</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The records filtered</returns>
        public async Task<IEnumerable<T>> GetRecordsByFilterAsync(Func<T, bool> filter, int start, int pageSize, string ago)
        {
            bool CombineFilter(T x) => filter(x) && x.Timestamp >= TimeStringParser.GetTimeAgo(ago);
            var allRecords = GetAllRecords();
            var data = allRecords.Where(CombineFilter).Page(start, pageSize);

            return await Task.FromResult(data);
        }

        /// <summary>
        /// Get the records and filter by a given predicate via observable
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="start">The start record</param>
        /// <param name="pageSize">The page size</param>
        /// <returns>The observable for the results</returns>
        public IObservable<T> GetRecordsByFilterObservable(Func<T, bool> filter, int start, int pageSize)
        {
            return Observable.Create<T>(o =>
            {
                foreach (var result in GetAllRecords().Where(filter).Page(start, pageSize))
                {
                    o.OnNext(result);
                }
                return Disposable.Empty;
            });
        }

        /// <summary>
        /// Get the records and filter by a given predicate via observable
        /// </summary>
        /// <param name="filter">The filter to apply</param>
        /// <param name="start">The start record</param>
        /// <param name="pageSize">The page size</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The observable for the results</returns>
        public IObservable<T> GetRecordsByFilterObservable(Func<T, bool> filter, int start, int pageSize, string ago)
        {
            bool CombineFilter(T x) => filter(x) && x.Timestamp >= TimeStringParser.GetTimeAgo(ago);

            return Observable.Create<T>(o =>
            {
                foreach (var result in GetAllRecords().Where(CombineFilter).Page(start, pageSize))
                {
                    o.OnNext(result);
                }
                return Disposable.Empty;
            });
        }

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public void Insert(T record)
        {
            EnsureRecord(record);
            CloudTable.AddEntity(record);
        }

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        public void Insert(IEnumerable<T> records)
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
                var addEntitiesBatch = new List<TableTransactionAction>();
                addEntitiesBatch.AddRange(entry.Select(e => new TableTransactionAction(TableTransactionActionType.Add, e)));

                CloudTable.SubmitTransaction(addEntitiesBatch);
            }
        }

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public Task InsertAsync(T record)
        {
            EnsureRecord(record);

            return CloudTable.AddEntityAsync(record);
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

            var partitionKeySeparation = records.GroupBy(x => x.PartitionKey)
                .OrderBy(g => g.Key)
                .Select(g => g.AsEnumerable()).SelectMany(entry => entry.Partition(MaxPartitionSize)).ToList();

            await foreach (var entry in partitionKeySeparation.ToAsyncEnumerable())
            {
                var addEntitiesBatch = new List<TableTransactionAction>();
                addEntitiesBatch.AddRange(entry.Select(e => new TableTransactionAction(TableTransactionActionType.Add, e)));
                await CloudTable.SubmitTransactionAsync(addEntitiesBatch).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        public void InsertOrReplace(T record)
        {
            EnsureRecord(record);

            CloudTable.UpsertEntity(record);
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        public Task InsertOrReplaceAsync(T record)
        {
            EnsureRecord(record);

            return CloudTable.UpsertEntityAsync(record);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public void Update(T record)
        {
            EnsureRecord(record);

            CloudTable.UpdateEntity(record, record.ETag);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public Task UpdateAsync(T record)
        {
            EnsureRecord(record);

            return CloudTable.UpdateEntityAsync(record, record.ETag);
        }

        /// <summary>
        /// Update an record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to update</param>
        public void UpdateUsingWildcardEtag(T record)
        {
            EnsureRecord(record);

            record.ETag = ETag.All;
            Update(record);
        }

        /// <summary>
        /// Update an record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to update</param>
        public Task UpdateUsingWildcardEtagAsync(T record)
        {
            EnsureRecord(record);

            record.ETag = ETag.All;
            return UpdateAsync(record);
        }

        #region Helpers

        /// <summary>
        /// Builds the get by partition query.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The table query</returns>
        //private static TableQuery<T> BuildGetByPartitionAndTimeQuery(string partitionKey, string ago)
        private Pageable<T> BuildGetByPartitionAndTimeQuery(string partitionKey, string ago)
        {
            var utcTime = new DateTimeOffset(TimeStringParser.GetTimeAgo(ago), TimeSpan.Zero);

            var queryResults = CloudTable.Query<T>(x => x.PartitionKey == partitionKey && x.Timestamp >= utcTime);
            return queryResults;
        }

        /// <summary>
        /// Build the row key table query
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <param name="ago">The time in the past to search e.g. 10m, 1h, etc.</param>
        /// <returns>The table query</returns>
        //private static TableQuery<T> BuildGetByRowKeyAndTimeQuery(string rowKey, string ago)
        private Pageable<T> BuildGetByRowKeyAndTimeQuery(string rowKey, string ago)
        {
            var utcTime = new DateTimeOffset(TimeStringParser.GetTimeAgo(ago), TimeSpan.Zero);

            var queryResults = CloudTable.Query<T>(x => x.RowKey == rowKey && x.Timestamp >= utcTime);
            return queryResults;
        }

        #endregion Helpers
    }
}