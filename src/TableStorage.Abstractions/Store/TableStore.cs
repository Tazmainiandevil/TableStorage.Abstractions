using Azure;
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

        #region Synchronous Methods

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public void Insert(T record)
        {
            EnsureRecord(record);
            CloudTable.AddEntity(record);
            //var operation = TableOperation.Insert(record);
            //CloudTable.Execute(operation);
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
                entry.ToList().ForEach(x => CloudTable.AddEntity(x));
                //var operation = new TableBatchOperation();
                //entry.ToList().ForEach(operation.Insert);

                //if (operation.Any())
                //{
                //    CloudTable.ExecuteBatch(operation);
                //}
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

            //var operation = TableOperation.InsertOrReplace(record);
            //CloudTable.Execute(operation);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public void Update(T record)
        {
            EnsureRecord(record);

            CloudTable.UpdateEntity(record, record.ETag, TableUpdateMode.Merge);
            //var operation = TableOperation.Merge(record);
            //CloudTable.Execute(operation);
        }

        /// <summary>
        /// Update an record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to update</param>
        public void UpdateUsingWildcardEtag(T record)
        {
            EnsureRecord(record);

            record.ETag = Azure.ETag.All;
            Update(record);
        }

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void Delete(T record)
        {
            EnsureRecord(record);

            CloudTable.DeleteEntity(record.PartitionKey, record.RowKey);

            //var operation = TableOperation.Delete(record);
            //CloudTable.Execute(operation);
        }

        /// <summary>
        /// Delete a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to delete</param>
        public void DeleteUsingWildcardEtag(T record)
        {
            EnsureRecord(record);

            record.ETag = Azure.ETag.All;
            Delete(record);
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

            // Create a retrieve operation that takes a customer record.
            //var retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            //// Execute the operation.
            //var retrievedResult = CloudTable.Execute(retrieveOperation);
            //return retrievedResult.Result as T;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByPartitionKey(string partitionKey)
        {
            EnsurePartitionKey(partitionKey);

            //TableContinuationToken continuationToken = null;

            var query = BuildGetByPartitionQuery(partitionKey);
            return query;

            //var allItems = new List<T>();

            //do
            //{
            //    var items = ExecuteQuerySegment(query, continuationToken);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            //return allItems;
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

            //TableContinuationToken continuationToken = null;

            var query = BuildGetByPartitionAndTimeQuery(partitionKey, ago);
            return query;

            //var allItems = new List<T>();
            //do
            //{
            //    var items = ExecuteQuerySegment(query, continuationToken);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            //return allItems;
        }

        ///// <summary>
        ///// Get the records by partition key, paged
        ///// </summary>
        ///// <param name="partitionKey">The partition key.</param>
        ///// <param name="pageSize">Size of the page.</param>
        ///// <param name="continuationTokenJson">The next page token.</param>
        ///// <returns>The Paged Result</returns>
        //public PagedResult<T> GetByPartitionKeyPaged(string partitionKey, int pageSize = 100, string continuationTokenJson = null)
        //{
        //    EnsurePartitionKey(partitionKey);

        //    var continuationToken = DeserializeContinuationToken(continuationTokenJson);

        //    var query = BuildGetByPartitionQuery(partitionKey);
        //    query.TakeCount = pageSize;

        //    var allItems = new List<T>();

        //    var items = ExecuteQuerySegment(query, continuationToken);
        //    continuationToken = items.ContinuationToken;
        //    allItems.AddRange(items);

        //    return CreatePagedResult(continuationToken, allItems);
        //}

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public IEnumerable<T> GetByRowKey(string rowKey)
        {
            EnsureRowKey(rowKey);

            //            TableContinuationToken continuationToken = null;

            var query = BuildGetByRowKeyQuery(rowKey);
            return query;

            //var allItems = new List<T>();
            //do
            //{
            //    var items = ExecuteQuerySegment(query, continuationToken);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            //return allItems;
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

            //TableContinuationToken continuationToken = null;

            var query = BuildGetByRowKeyAndTimeQuery(rowKey, ago);
            return query;

            //var allItems = new List<T>();
            //do
            //{
            //    var items = ExecuteQuerySegment(query, continuationToken);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            //return allItems;
        }

        ///// <summary>
        ///// Get the records by row key
        ///// </summary>
        ///// <param name="rowKey">The row key.</param>
        ///// <param name="pageSize">Size of the page.</param>
        ///// <param name="continuationTokenJson">The next page token.</param>
        ///// <returns>The Paged Result</returns>
        //public PagedResult<T> GetByRowKeyPaged(string rowKey, int pageSize = 100, string continuationTokenJson = null)
        //{
        //    EnsureRowKey(rowKey);

        //    TableContinuationToken continuationToken = DeserializeContinuationToken(continuationTokenJson);

        //    var query = BuildGetByRowKeyQuery(rowKey);
        //    query.TakeCount = pageSize;

        //    var allItems = new List<T>();

        //    var items = ExecuteQuerySegment(query, continuationToken);
        //    continuationToken = items.ContinuationToken;
        //    allItems.AddRange(items);

        //    return CreatePagedResult(continuationToken, allItems);
        //}

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

            //var query = new TableQuery<T>();

            //var token = new TableContinuationToken();
            //var segment = CloudTable.ExecuteQuerySegmented(query, token);
            //while (token != null)
            //{
            //    foreach (var result in segment)
            //    {
            //        yield return result;
            //    }
            //    token = segment.ContinuationToken;
            //    segment = CloudTable.ExecuteQuerySegmented(query, token);
            //}
        }

        ///// <summary>
        ///// Gets all records in the table, paged
        ///// </summary>
        ///// <param name="pageSize">Size of the page.</param>
        ///// <param name="pageToken">The page token</param>
        ///// <returns>The Paged Result</returns>

        //public PagedResult<T> GetAllRecordsPaged(int pageSize = 100, string pageToken = null)
        //{
        //    var query = new TableQuery<T> { TakeCount = pageSize };

        //    var allItems = new List<T>();
        //    var continuationToken = DeserializeContinuationToken(pageToken);
        //    var items = CloudTable.ExecuteQuerySegmented(query, continuationToken);
        //    continuationToken = items.ContinuationToken;
        //    allItems.AddRange(items);
        //    return CreatePagedResult(continuationToken, allItems);
        //}

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

        #endregion Synchronous Methods

        #region Asynchronous Methods

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        public async Task InsertAsync(T record)
        {
            EnsureRecord(record);

            await CloudTable.AddEntityAsync(record);

            //var operation = TableOperation.Insert(record);

            //await CloudTable.ExecuteAsync(operation).ConfigureAwait(false);
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

            foreach (var entry in partitionKeySeparation)
            {
                await foreach (T qEntity in entry.ToAsyncEnumerable())
                {
                    await CloudTable.AddEntityAsync(qEntity);
                }

                //var operation = new TableBatchOperation();
                //entry.ToList().ForEach(operation.Insert);

                //if (operation.Any())
                //{
                //    await CloudTable.ExecuteBatchAsync(operation).ConfigureAwait(false);
                //}
            }
        }

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(T record)
        {
            EnsureRecord(record);

            await CloudTable.UpsertEntityAsync(record);

            //var operation = TableOperation.InsertOrReplace(record);

            //await CloudTable.ExecuteAsync(operation).ConfigureAwait(false);
        }

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        public async Task UpdateAsync(T record)
        {
            EnsureRecord(record);

            await CloudTable.UpdateEntityAsync(record, record.ETag);

            //var operation = TableOperation.Merge(record);

            //await CloudTable.ExecuteAsync(operation).ConfigureAwait(false);
        }

        /// <summary>
        /// Update an record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to update</param>
        public async Task UpdateUsingWildcardEtagAsync(T record)
        {
            EnsureRecord(record);

            //record.ETag = "*";
            record.ETag = ETag.All;
            await UpdateAsync(record).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete an entry
        /// </summary>
        /// <param name="record">The record to delete</param>
        public async Task DeleteAsync(T record)
        {
            EnsureRecord(record);

            await CloudTable.DeleteEntityAsync(record.PartitionKey, record.RowKey);
            //var operation = TableOperation.Delete(record);

            //await CloudTable.ExecuteAsync(operation).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to delete</param>
        public async Task DeleteUsingWildcardEtagAsync(T record)
        {
            EnsureRecord(record);

            //record.ETag = "*";
            record.ETag = ETag.All;

            await DeleteAsync(record).ConfigureAwait(false);
        }

        /// <summary>
        /// Delete records by partition key
        /// </summary>
        /// <param name="partitionKey"></param>
        public async Task DeleteByPartitionAsync(string partitionKey)
        {
            var deleteQuery = BuildGetByPartitionQuery(partitionKey);

            await foreach (var item in deleteQuery.ToAsyncEnumerable())
            {
                await CloudTable.DeleteEntityAsync(item.PartitionKey, item.RowKey);
            }

            //TableContinuationToken continuationToken = null;
            //do
            //{
            //    var tableQueryResult = CloudTable.ExecuteQuerySegmentedAsync(deleteQuery, continuationToken);
            //    continuationToken = tableQueryResult.Result.ContinuationToken;

            //    // Split result into chunks of 100s
            //    var rowsChunked = tableQueryResult.Result.ToList().Partition(100);

            //    // Delete each chunk in a batch
            //    foreach (var rows in rowsChunked)
            //    {
            //        TableBatchOperation tableBatchOperation = new TableBatchOperation();
            //        foreach (var row in rows)
            //        {
            //            tableBatchOperation.Add(TableOperation.Delete(row));
            //        }
            //        await CloudTable.ExecuteBatchAsync(tableBatchOperation);
            //    }
            //}
            //while (continuationToken != null);
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

            //// Create a retrieve operation that takes a customer record.
            //var retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            //// Execute the operation.
            //var retrievedResult = await CloudTable.ExecuteAsync(retrieveOperation).ConfigureAwait(false);

            //return retrievedResult.Result as T;
        }

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByPartitionKeyAsync(string partitionKey)
        {
            EnsurePartitionKey(partitionKey);

            var allItems = new List<T>();
            AsyncPageable<T> queryResults = CloudTable.QueryAsync<T>(filter: $"PartitionKey eq '{partitionKey}'");
            await foreach (var queryResult in queryResults)
            {
                allItems.Add(queryResult);
            }
            //TableContinuationToken continuationToken = null;

            //var query = BuildGetByPartitionQuery(partitionKey);

            //var allItems = new List<T>();
            //do
            //{
            //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            return allItems;
        }

        public async Task<IEnumerable<T>> GetByPartitionKeyAsync(string partitionKey, string ago)
        {
            EnsurePartitionKey(partitionKey);

            var allItems = new List<T>();
            var utcTime = new DateTimeOffset(TimeStringParser.GetTimeAgo(ago), TimeSpan.Zero);

            AsyncPageable<T> queryResults = CloudTable.QueryAsync<T>(x => x.PartitionKey == partitionKey && x.Timestamp >= utcTime);
            await foreach (var queryResult in queryResults)
            {
                allItems.Add(queryResult);
            }

            //            TableContinuationToken continuationToken = null;

            //            var query = BuildGetByPartitionAndTimeQuery(partitionKey, ago);

            //var allItems = new List<T>();
            //do
            //{
            //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            return allItems;
        }

        ///// <summary>
        /////  Get the records by partition key, paged
        ///// </summary>
        ///// <param name="partitionKey">The partition key.</param>
        ///// <param name="pageSize">Size of the page.</param>
        ///// <param name="continuationTokenJson">The next page token.</param>
        ///// <returns>The Paged Result</returns>
        //public async Task<PagedResult<T>> GetByPartitionKeyPagedAsync(string partitionKey, int pageSize = 100, string continuationTokenJson = null)
        //{
        //    EnsurePartitionKey(partitionKey);

        //    var continuationToken = DeserializeContinuationToken(continuationTokenJson);

        //    var query = BuildGetByPartitionQuery(partitionKey);
        //    query.TakeCount = pageSize;

        //    var allItems = new List<T>();

        //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
        //    continuationToken = items.ContinuationToken;
        //    allItems.AddRange(items);

        //    return CreatePagedResult(continuationToken, allItems);
        //}

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        public async Task<IEnumerable<T>> GetByRowKeyAsync(string rowKey)
        {
            EnsureRowKey(rowKey);

            //TableContinuationToken continuationToken = null;

            var allItems = new List<T>();
            AsyncPageable<T> queryResults = CloudTable.QueryAsync<T>(filter: $"RowKey eq '{rowKey}'");
            await foreach (var queryResult in queryResults)
            {
                allItems.Add(queryResult);
            }

            //var allItems = new List<T>();
            //do
            //{
            //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            return allItems;
        }

        public async Task<IEnumerable<T>> GetByRowKeyAsync(string rowKey, string ago)
        {
            EnsureRowKey(rowKey);

            var allItems = new List<T>();
            var utcTime = new DateTimeOffset(TimeStringParser.GetTimeAgo(ago), TimeSpan.Zero);

            AsyncPageable<T> queryResults = CloudTable.QueryAsync<T>(x => x.RowKey == rowKey && x.Timestamp >= utcTime);
            await foreach (var queryResult in queryResults)
            {
                allItems.Add(queryResult);
            }

            //TableContinuationToken continuationToken = null;

            //var query = BuildGetByRowKeyAndTimeQuery(rowKey, ago);

            //var allItems = new List<T>();
            //do
            //{
            //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            return allItems;
        }

        ///// <summary>
        ///// Get the records by row key
        ///// </summary>
        ///// <param name="rowKey">The row key.</param>
        ///// <param name="pageSize">Size of the page.</param>
        ///// <param name="continuationTokenJson">The next page token.</param>
        //public async Task<PagedResult<T>> GetByRowKeyPagedAsync(string rowKey, int pageSize = 100, string continuationTokenJson = null)
        //{
        //    EnsureRowKey(rowKey);

        //    var continuationToken = DeserializeContinuationToken(continuationTokenJson);

        //    var query = BuildGetByRowKeyQuery(rowKey);
        //    query.TakeCount = pageSize;

        //    var allItems = new List<T>();

        //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
        //    continuationToken = items.ContinuationToken;
        //    allItems.AddRange(items);

        //    return CreatePagedResult(continuationToken, allItems);
        //}

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        public async Task<IEnumerable<T>> GetAllRecordsAsync()
        {
            var allItems = new List<T>();

            var queryResults = CloudTable.QueryAsync<T>();
            await foreach (var queryResult in queryResults)
            {
                allItems.Add(queryResult);
            }

            //TableContinuationToken continuationToken = null;

            //var query = new TableQuery<T>();

            //var allItems = new List<T>();
            //do
            //{
            //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
            //    continuationToken = items.ContinuationToken;
            //    allItems.AddRange(items);
            //} while (continuationToken != null);

            return allItems;
        }

        ///// <summary>
        ///// Gets all records in the table, paged
        ///// </summary>
        ///// <returns>The Paged Result</returns>
        //public async Task<PagedResult<T>> GetAllRecordsPagedAsync(int pageSize = 100, string pageToken = null)
        //{
        //    var query = new TableQuery<T> { TakeCount = pageSize };

        //    var allItems = new List<T>();
        //    var continuationToken = DeserializeContinuationToken(pageToken);
        //    var items = await CloudTable.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
        //    continuationToken = items.ContinuationToken;
        //    allItems.AddRange(items);
        //    return CreatePagedResult(continuationToken, allItems);
        //}

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

        #endregion Asynchronous Methods

        #region Helpers

        /// <summary>
        /// Builds the get by partition query.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <returns>The table query</returns>
        //private static TableQuery<T> BuildGetByPartitionQuery(string partitionKey)
        private Pageable<T> BuildGetByPartitionQuery(string partitionKey)
        {
            Pageable<T> queryResults = CloudTable.Query<T>(filter: $"PartitionKey eq '{partitionKey}'");
            return queryResults;
            //var query = new TableQuery<T>().Where(
            //    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            //return query;
        }

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

            Pageable<T> queryResults = CloudTable.Query<T>(x => x.PartitionKey == partitionKey && x.Timestamp >= utcTime);
            return queryResults;

            //var query = new TableQuery<T>().Where(TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
            //                                                                TableOperators.And,
            //                                                                TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThanOrEqual, utcTime)));
            //return query;
        }

        /// <summary>
        /// Build the row key table query
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The table query</returns>
        //private static TableQuery<T> BuildGetByRowKeyQuery(string rowKey)
        private Pageable<T> BuildGetByRowKeyQuery(string rowKey)
        {
            Pageable<T> queryResults = CloudTable.Query<T>(filter: $"RowKey eq '{rowKey}'");
            return queryResults;
            //var query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));
            //return query;
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

            Pageable<T> queryResults = CloudTable.Query<T>(x => x.RowKey == rowKey && x.Timestamp >= utcTime);
            return queryResults;
            //var query = new TableQuery<T>().Where(TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey),
            //                                                                TableOperators.And,
            //                                                                TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThanOrEqual, utcTime)));
            //return query;
        }

        ///// <summary>
        ///// Create a paged result
        ///// </summary>
        ///// <param name="continuationToken">The continuation token</param>
        ///// <param name="items">The items</param>
        ///// <returns>The paged result</returns>
        //private static PagedResult<T> CreatePagedResult(TableContinuationToken continuationToken, IList<T> items)
        //{
        //    var continuationTokenJson = continuationToken != null ? JsonConvert.SerializeObject(continuationToken) : null;
        //    return new PagedResult<T>(items, continuationTokenJson, continuationToken == null);
        //}

        ///// <summary>
        ///// Deserialize the continuation token
        ///// </summary>
        ///// <param name="continuationTokenJson">The json string containing the continuation token</param>
        ///// <returns>The continuation token</returns>
        //private static TableContinuationToken DeserializeContinuationToken(string continuationTokenJson)
        //{
        //    TableContinuationToken continuationToken = null;
        //    if (!string.IsNullOrEmpty(continuationTokenJson))
        //    {
        //        continuationToken = JsonConvert.DeserializeObject<TableContinuationToken>(continuationTokenJson);
        //    }
        //    return continuationToken;
        //}

        //private TableQuerySegment<T> ExecuteQuerySegment(TableQuery<T> query, TableContinuationToken continuationToken)
        //{
        //    var items = CloudTable.ExecuteQuerySegmented(query, continuationToken);
        //    return items;
        //}

        //private Pageable<T> ExecuteQuerySegment(TableQuery<T> query, TableContinuationToken continuationToken)
        //{
        //    var items = CloudTable.ExecuteQuerySegmented(query, continuationToken);
        //    return items;
        //}

        #endregion Helpers
    }
}