using System.Collections.Generic;
using System.Threading.Tasks;

namespace TableStorage.Abstractions
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITableStore<T>
    {
        #region Synchronous Methods

        /// <summary>
        /// Create the table
        /// </summary>
        void CreateTable();

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns></returns>
        bool TableExists();

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        void Insert(T record);

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        void Insert(IEnumerable<T> records);

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        void Update(T record);

        /// <summary>
        /// Update a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to update</param>
        void UpdateUsingWildcardEtag(T record);

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        void Delete(T record);

        /// <summary>
        /// Delete a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to delete</param>
        void DeleteUsingWildcardEtag(T record);

        /// <summary>
        /// Delete the table
        /// </summary>
        void DeleteTable();

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        T GetRecord(string partitionKey, string rowKey);

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        IEnumerable<T> GetByPartitionKey(string partitionKey);

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        IEnumerable<T> GetByRowKey(string rowKey);

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        IEnumerable<T> GetAllRecords();

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        int GetRecordCount();

        #endregion Synchronous Methods

        #region Asynchronous Methods

        /// <summary>
        /// Create the table
        /// </summary>
        Task CreateTableAsync();

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns></returns>
        Task<bool> TableExistsAsync();

        /// <summary>
        /// Insert an record
        /// </summary>
        /// <param name="record">The record to insert</param>
        Task InsertAsync(T record);

        /// <summary>
        /// Insert multiple records
        /// </summary>
        /// <param name="records">The records to insert</param>
        Task InsertAsync(IEnumerable<T> records);

        /// <summary>
        /// Update an record
        /// </summary>
        /// <param name="record">The record to update</param>
        Task UpdateAsync(T record);

        /// <summary>
        /// Update an record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to update</param>
        Task UpdateUsingWildcardEtagAsync(T record);

        /// <summary>
        /// Delete a record
        /// </summary>
        /// <param name="record">The record to delete</param>
        Task DeleteAsync(T record);

        /// <summary>
        /// Delete a record using the wildcard etag
        /// </summary>
        /// <param name="record">The record to delete</param>
        Task DeleteUsingWildcardEtagAsync(T record);

        /// <summary>
        /// Delete the table
        /// </summary>
        Task DeleteTableAsync();

        /// <summary>
        /// Get an record by partition and row key
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>The record found or null if not found</returns>
        Task<T> GetRecordAsync(string partitionKey, string rowKey);

        /// <summary>
        /// Get the records by partition key
        /// </summary>
        /// <param name="partitionKey">The partition key</param>
        /// <returns>The records found</returns>
        Task<IEnumerable<T>> GetByPartitionKeyAsync(string partitionKey);

        /// <summary>
        /// Get the records by row key
        /// </summary>
        /// <param name="rowKey">The row key</param>
        /// <returns>The records found</returns>
        Task<IEnumerable<T>> GetByRowKeyAsync(string rowKey);

        /// <summary>
        /// Get all the records in the table
        /// </summary>
        /// <returns>All records</returns>
        Task<IEnumerable<T>> GetAllRecordsAsync();

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        Task<int> GetRecordCountAsync();


        #endregion Asynchronous Methods
    }
}