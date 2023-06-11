using Azure;
using Azure.Core;
using Azure.Data.Tables;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Factory
{
    public class TableStoreFactory : ITableStoreFactory
    {
        #region Create Store

        public ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(tableName, storageConnectionString, new TableStorageOptions());
        }

        public ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString, TableStorageOptions options) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(tableName, storageConnectionString, options);
        }

        public ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TokenCredential tokenCredential) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(accountName, tableName, tokenCredential, new TableStorageOptions());
        }

        public ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TokenCredential tokenCredential, TableStorageOptions options) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(accountName, tableName, tokenCredential, options);
        }

        public ITableStore<T> CreateTableStore<T>(string accountName, string tableName, AzureSasCredential sasCredential) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(accountName, tableName, sasCredential);
        }

        public ITableStore<T> CreateTableStore<T>(string accountName, string tableName, AzureSasCredential sasCredential,
            TableStorageOptions options) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(accountName, tableName, sasCredential, options);
        }

        public ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(accountName, tableName, sharedKeyCredential);
        }

        public ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential,
            TableStorageOptions options) where T : class, ITableEntity, new()
        {
            return new TableStore<T>(accountName, tableName, sharedKeyCredential, options);
        }

        #endregion Create Store

        #region Create Dynamic Store

        public ITableStoreDynamic CreateTableStore(string tableName, string storageConnectionString)
        {
            return new TableStoreDynamic(tableName, storageConnectionString);
        }

        public ITableStoreDynamic CreateTableStore(string tableName, string storageConnectionString, TableStorageOptions options)
        {
            return new TableStoreDynamic(tableName, storageConnectionString, options);
        }

        public ITableStoreDynamic CreateTableStore(string accountName, string tableName, TokenCredential tokenCredential)
        {
            return new TableStoreDynamic(accountName, tableName, tokenCredential);
        }

        public ITableStoreDynamic CreateTableStore(string accountName, string tableName, TokenCredential tokenCredential, TableStorageOptions options)
        {
            return new TableStoreDynamic(accountName, tableName, tokenCredential, options);
        }

        public ITableStoreDynamic CreateTableStore(string accountName, string tableName, AzureSasCredential sasCredential)
        {
            return new TableStoreDynamic(accountName, tableName, sasCredential);
        }

        public ITableStoreDynamic CreateTableStore(string accountName, string tableName, AzureSasCredential sasCredential,
            TableStorageOptions options)
        {
            return new TableStoreDynamic(accountName, tableName, sasCredential, options);
        }

        public ITableStoreDynamic CreateTableStore(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential)
        {
            return new TableStoreDynamic(accountName, tableName, sharedKeyCredential);
        }

        public ITableStoreDynamic CreateTableStore(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential,
            TableStorageOptions options)
        {
            return new TableStoreDynamic(accountName, tableName, sharedKeyCredential, options);
        }

        #endregion Create Dynamic Store
    }
}