using Azure;
using Azure.Core;
using Azure.Data.Tables;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Factory
{
    public interface ITableStoreFactory
    {
        #region Create Store

        ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string tableName, string storageConnectionString, TableStorageOptions options) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TokenCredential tokenCredential) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TokenCredential tokenCredential, TableStorageOptions options) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string accountName, string tableName, AzureSasCredential sasCredential) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string accountName, string tableName, AzureSasCredential sasCredential, TableStorageOptions options) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential) where T : class, ITableEntity, new();

        ITableStore<T> CreateTableStore<T>(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential, TableStorageOptions options) where T : class, ITableEntity, new();

        #endregion Create Store

        #region Create Dynamic Store

        ITableStoreDynamic CreateTableStore(string tableName, string storageConnectionString);

        ITableStoreDynamic CreateTableStore(string tableName, string storageConnectionString, TableStorageOptions options);

        ITableStoreDynamic CreateTableStore(string accountName, string tableName, TokenCredential tokenCredential);

        ITableStoreDynamic CreateTableStore(string accountName, string tableName, TokenCredential tokenCredential, TableStorageOptions options);

        ITableStoreDynamic CreateTableStore(string accountName, string tableName, AzureSasCredential sasCredential);

        ITableStoreDynamic CreateTableStore(string accountName, string tableName, AzureSasCredential sasCredential, TableStorageOptions options);

        ITableStoreDynamic CreateTableStore(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential);

        ITableStoreDynamic CreateTableStore(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential, TableStorageOptions options);

        #endregion Create Dynamic Store
    }
}