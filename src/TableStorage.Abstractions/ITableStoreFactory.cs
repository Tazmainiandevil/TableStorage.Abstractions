using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorage.Abstractions
{
    public interface ITableStoreFactory
    {
        ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString) where T : TableEntity, new();
        ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString, int retries, double retryWaitTimeInSeconds) where T : TableEntity, new();
    }
}