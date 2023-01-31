using System.Threading;
using System.Threading.Tasks;

namespace TableStorage.Abstractions.Store
{
    public interface ITableStoreCommon
    {
        /// <summary>
        /// Create the table
        /// </summary>
        void CreateTable();

        /// <summary>
        /// Create the table
        /// </summary>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task CreateTableAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns>A boolean denoting if the table exists</returns>
        bool TableExists();

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        /// <returns>A boolean denoting if the table exists</returns>
        Task<bool> TableExistsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Delete the table
        /// </summary>
        void DeleteTable();

        /// <summary>
        /// Delete the table
        /// </summary>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        Task DeleteTableAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        int GetRecordCount();

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <param name="cancellationToken">Used to cancel the operation</param>
        /// <returns>The record count</returns>
        Task<int> GetRecordCountAsync(CancellationToken cancellationToken = default);
    }
}