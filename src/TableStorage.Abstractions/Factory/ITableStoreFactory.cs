using Microsoft.Azure.Cosmos.Table;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Factory
{
    public interface ITableStoreFactory
    {
        ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString, TableStorageOptions options) where T : class, ITableEntity, new();
    }
}