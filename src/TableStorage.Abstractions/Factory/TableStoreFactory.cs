using Azure.Data.Tables;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Factory
{
    public class TableStoreFactory : ITableStoreFactory
    {
        public ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(tableName, storageConnectionString, new TableStorageOptions());
        }

        public ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString, TableStorageOptions options) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(tableName, storageConnectionString, options);
        }

        public ITableStoreDynamic CreateTableStore(string tableName, string storageConnectionString)
        {
            return new TableStoreDynamic(tableName, storageConnectionString);
        }

        public ITableStoreDynamic CreateTableStore(string tableName, string storageConnectionString, TableStorageOptions options)
        {
            return new TableStoreDynamic(tableName, storageConnectionString, options);
        }
    }
}