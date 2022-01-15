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
        Task CreateTableAsync();

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns>A boolean denoting if the table exists</returns>
        bool TableExists();

        /// <summary>
        /// Does the table exist
        /// </summary>
        /// <returns>A boolean denoting if the table exists</returns>
        Task<bool> TableExistsAsync();

        /// <summary>
        /// Delete the table
        /// </summary>
        void DeleteTable();

        /// <summary>
        /// Delete the table
        /// </summary>
        Task DeleteTableAsync();

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        int GetRecordCount();

        /// <summary>
        /// Get the number of the records in the table
        /// </summary>
        /// <returns>The record count</returns>
        Task<int> GetRecordCountAsync();
    }
}