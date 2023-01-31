using Azure.Data.Tables;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TableStorage.Abstractions.Store
{
    public interface ITableStoreDynamic : ITableStoreCommon
    {

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        void Insert<T>(T record) where T : class, ITableEntity, new();

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task InsertAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        void Insert<T>(IEnumerable<T> records) where T : class, ITableEntity, new();

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task InsertAsync<T>(IEnumerable<T> records, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        void InsertOrReplace<T>(T record) where T : class, ITableEntity, new();

        /// <summary>
        /// Inserts or replaces the record
        /// </summary>
        /// <param name="record"></param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task InsertOrReplaceAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        void Update<T>(T record) where T : class, ITableEntity, new();

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task UpdateAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        void Delete<T>(T record) where T : class, ITableEntity;

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task DeleteAsync<T>(T record, CancellationToken cancellationToken = default) where T : class, ITableEntity;

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        T GetRecord<T>(string partitionKey, string rowKey) where T : class, ITableEntity, new();

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task<T> GetRecordAsync<T>(string partitionKey, string rowKey, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        IEnumerable<TableEntity> GetAllRecords();

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        /// <returns>All records</returns>
        Task<IEnumerable<TableEntity>> GetAllRecordsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        IEnumerable<T> GetByPartitionKey<T>(string partitionKey) where T : class, ITableEntity, new();

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        /// <returns>The records found</returns>
        Task<IEnumerable<T>> GetByPartitionKeyAsync<T>(string partitionKey, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        IEnumerable<T> GetByRowKey<T>(string rowKey) where T : class, ITableEntity, new();

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        /// <returns>The records found</returns>
        Task<IEnumerable<T>> GetByRowKeyAsync<T>(string rowKey, CancellationToken cancellationToken = default) where T : class, ITableEntity, new();
    }
}