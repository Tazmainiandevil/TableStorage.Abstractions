using Azure;
using Azure.Data.Tables;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using TableStorage.Abstractions.Validators;

namespace TableStorage.Abstractions.Store;

public class TableStoreBase
{
    /// <summary>
    /// The max size for a single partition to be added to Table Storage
    /// </summary>
    protected const int MaxPartitionSize = 100;

    /// <summary>
    /// The cloud table
    /// </summary>
    protected readonly TableClient CloudTable;

    private readonly TableServiceClient _cloudTableService;
    private readonly string _tableName;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="tableName">The table name</param>
    /// <param name="storageConnectionString">The connection string</param>
    protected TableStoreBase(string tableName, string storageConnectionString) : this(tableName, storageConnectionString, new TableStorageOptions())
    {
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="tableName">The table name</param>
    /// <param name="storageConnectionString">The connection string</param>
    /// <param name="options">Table storage options</param>
    protected TableStoreBase(string tableName, string storageConnectionString, TableStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        }

        if (string.IsNullOrWhiteSpace(storageConnectionString))
        {
            throw new ArgumentException("Table connection string cannot be null or empty", nameof(storageConnectionString));
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options), "Table storage options cannot be null");
        }

        var validator = new TableStorageOptionsValidator();
        validator.ValidateAndThrow(options);

        OptimisePerformance(storageConnectionString, options);
        (_cloudTableService, CloudTable) = CreateTableClient(storageConnectionString, tableName, options.Retries, options.RetryWaitTimeInSeconds);

        _tableName = tableName;

        if (options.EnsureTableExists)
        {
            //if (!TableExists())
            //{
            CreateTable();
            //}
        }
    }

    /// <summary>
    /// Settings to improve performance
    /// </summary>
    private static void OptimisePerformance(string storageConnectionString, TableStorageOptions options)
    {
        var endpoint = ParseConnectionString.GetTableEndpoint(storageConnectionString);
        var tableServicePoint = ServicePointManager.FindServicePoint(endpoint);
        tableServicePoint.UseNagleAlgorithm = options.UseNagleAlgorithm;
        tableServicePoint.Expect100Continue = options.Expect100Continue;
        tableServicePoint.ConnectionLimit = options.ConnectionLimit;
    }

    /// <summary>
    /// Create the table client
    /// </summary>
    /// <param name="connectionString">The connection string</param>
    /// <param name="tableName">The name of the table</param>
    /// <param name="retries">Number of retries</param>
    /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
    /// <returns>The table client</returns>
    private static (TableServiceClient serviceClient, TableClient tableClient) CreateTableClient(string connectionString, string tableName, int retries, double retryWaitTimeInSeconds)
    {
        var options = new TableClientOptions
        {
            Retry =
            {
                MaxRetries = retries,
                Delay = TimeSpan.FromSeconds(retryWaitTimeInSeconds),
                Mode = Azure.Core.RetryMode.Exponential
            }
        };

        var serviceClient = new TableServiceClient(connectionString, options);
        var tableClient = serviceClient.GetTableClient(tableName);
        return (serviceClient, tableClient);
    }

    /// <summary>
    /// Create the table
    /// </summary>
    public void CreateTable()
    {
        CloudTable.CreateIfNotExists();
    }

    /// <summary>
    /// Create the table
    /// </summary>
    public Task CreateTableAsync()
    {
        return CloudTable.CreateIfNotExistsAsync();
    }

    /// <summary>
    /// Does the table exist
    /// </summary>
    /// <returns></returns>
    public bool TableExists()
    {
        return _cloudTableService.Query(e => e.Name == _tableName).Any();
    }

    /// <summary>
    /// Does the table exist
    /// </summary>
    /// <returns></returns>
    public async Task<bool> TableExistsAsync()
    {
        return (await _cloudTableService.QueryAsync(e => e.Name == _tableName).ToListAsync()).Any();
    }

    /// <summary>
    /// Delete the table
    /// </summary>
    public void DeleteTable()
    {
        CloudTable.Delete();
    }

    /// <summary>
    /// Delete the table
    /// </summary>
    public Task DeleteTableAsync()
    {
        return CloudTable.DeleteAsync();
    }

    /// <summary>
    /// Get the number of the records in the table
    /// </summary>
    /// <returns>The record count</returns>
    public int GetRecordCount()
    {
        return CloudTable.Query<TableEntity>(select: new List<string> { "PartitionKey" }).Count();
    }

    /// <summary>
    /// Get the number of the records in the table
    /// </summary>
    /// <returns>The record count</returns>
    public async Task<int> GetRecordCountAsync()
    {
        return await (CloudTable.QueryAsync<TableEntity>(select: new List<string> { "PartitionKey" })).CountAsync();
    }

    #region Helpers

    /// <summary>
    /// Ensures the partition key is not null.
    /// </summary>
    /// <param name="partitionKey">The partition key.</param>
    /// <exception cref="ArgumentNullException">partitionKey</exception>
    protected void EnsurePartitionKey(string partitionKey)
    {
        if (string.IsNullOrWhiteSpace(partitionKey))
        {
            throw new ArgumentNullException(nameof(partitionKey), "PartitionKey cannot be null or empty");
        }
    }

    /// <summary>
    /// Ensures the row key is not null.
    /// </summary>
    /// <param name="rowKey">The row key.</param>
    /// <exception cref="ArgumentNullException">rowKey</exception>
    protected void EnsureRowKey(string rowKey)
    {
        if (string.IsNullOrWhiteSpace(rowKey))
        {
            throw new ArgumentNullException(nameof(rowKey), "RowKey cannot be null or empty");
        }
    }

    protected void EnsureRecord<T>(T record)
    {
        if (record == null)
        {
            throw new ArgumentNullException(nameof(record), "Record cannot be null");
        }
    }

    /// <summary>
    /// Builds the get by partition query.
    /// </summary>
    /// <param name="partitionKey">The partition key.</param>
    /// <returns>The table query</returns>
    //private static TableQuery<T> BuildGetByPartitionQuery(string partitionKey)
    protected Pageable<T> BuildGetByPartitionQuery<T>(string partitionKey) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.Query<T>(filter: $"PartitionKey eq '{partitionKey}'");
        return queryResults;
    }

    protected AsyncPageable<T> BuildGetByPartitionQueryAsync<T>(string partitionKey) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.QueryAsync<T>(filter: $"PartitionKey eq '{partitionKey}'");
        return queryResults;
    }

    /// <summary>
    /// Build the row key table query
    /// </summary>
    /// <param name="rowKey">The row key</param>
    /// <returns>The table query</returns>
    protected Pageable<T> BuildGetByRowKeyQuery<T>(string rowKey) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.Query<T>(filter: $"RowKey eq '{rowKey}'");
        return queryResults;
    }

    protected AsyncPageable<T> BuildGetByRowKeyQueryAsync<T>(string rowKey) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.QueryAsync<T>(filter: $"RowKey eq '{rowKey}'");
        return queryResults;
    }

    #endregion Helpers
}