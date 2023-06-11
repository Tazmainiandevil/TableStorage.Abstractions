﻿using Azure;
using Azure.Core;
using Azure.Data.Tables;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
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

    #region Connection String Construction

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

        var endpoint = ParseConnectionString.GetTableEndpoint(storageConnectionString);
        OptimisePerformance(endpoint, options);
        (_cloudTableService, CloudTable) = CreateTableClient(storageConnectionString, tableName, options.Retries, options.RetryWaitTimeInSeconds);

        _tableName = tableName;

        if (options.EnsureTableExists)
        {
            CreateTable();
        }
    }

    #endregion Connection String Construction

    #region Token Credential Construction

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="accountName">The table account name</param>
    /// <param name="tableName">The table name</param>
    /// <param name="tokenCredential">The connection using token credentials</param>
    protected TableStoreBase(string accountName, string tableName, TokenCredential tokenCredential) : this(accountName, tableName, tokenCredential, new TableStorageOptions())
    {
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="accountName">The table account name</param>
    /// <param name="tableName">The table name</param>
    /// <param name="tokenCredential">The connection using token credentials</param>
    /// <param name="options">Table storage options</param>
    protected TableStoreBase(string accountName, string tableName, TokenCredential tokenCredential, TableStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(accountName))
        {
            throw new ArgumentException("Account name cannot be null or empty", nameof(accountName));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        }

        if (tokenCredential == null)
        {
            throw new ArgumentNullException(nameof(tokenCredential), "TokenCredential cannot be null");
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options), "Table storage options cannot be null");
        }

        var validator = new TableStorageOptionsValidator();
        validator.ValidateAndThrow(options);

        var tableEndpoint = $"https://{accountName}.table.core.windows.net/";

        OptimisePerformance(new Uri(tableEndpoint), options);
        (_cloudTableService, CloudTable) = CreateTableClient(tableEndpoint, tokenCredential, tableName, options.Retries, options.RetryWaitTimeInSeconds);

        _tableName = tableName;

        if (options.EnsureTableExists)
        {
            CreateTable();
        }
    }

    #endregion Token Credential Construction

    #region Sas Credential Construction

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="accountName">The table account name</param>
    /// <param name="tableName">The table name</param>
    /// <param name="sasCredential">The connection using sas credentials</param>
    protected TableStoreBase(string accountName, string tableName, AzureSasCredential sasCredential) : this(accountName, tableName, sasCredential, new TableStorageOptions())
    {
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="accountName">The table account name</param>
    /// <param name="tableName">The table name</param>
    /// <param name="sasCredential">The connection using sas credentials</param>
    /// <param name="options">Table storage options</param>
    protected TableStoreBase(string accountName, string tableName, AzureSasCredential sasCredential, TableStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(accountName))
        {
            throw new ArgumentException("Account name cannot be null or empty", nameof(accountName));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        }

        if (sasCredential == null)
        {
            throw new ArgumentNullException(nameof(sasCredential), "AzureSasCredential cannot be null");
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options), "Table storage options cannot be null");
        }

        var validator = new TableStorageOptionsValidator();
        validator.ValidateAndThrow(options);

        var tableEndpoint = $"https://{accountName}.table.core.windows.net/";

        OptimisePerformance(new Uri(tableEndpoint), options);
        (_cloudTableService, CloudTable) = CreateTableClient(tableEndpoint, sasCredential, tableName, options.Retries, options.RetryWaitTimeInSeconds);

        _tableName = tableName;

        if (options.EnsureTableExists)
        {
            CreateTable();
        }
    }

    #endregion Sas Credential Construction

    #region Table SharedKey Credential Construction

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="accountName">The table account name</param>
    /// <param name="tableName">The table name</param>
    /// <param name="sharedKeyCredential">The connection using shared key credentials</param>
    protected TableStoreBase(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential) : this(accountName, tableName, sharedKeyCredential, new TableStorageOptions())
    {
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="accountName">The table account name</param>
    /// <param name="tableName">The table name</param>
    /// <param name="sharedKeyCredential">The connection using shared key credentials</param>
    /// <param name="options">Table storage options</param>
    protected TableStoreBase(string accountName, string tableName, TableSharedKeyCredential sharedKeyCredential, TableStorageOptions options)
    {
        if (string.IsNullOrWhiteSpace(accountName))
        {
            throw new ArgumentException("Account name cannot be null or empty", nameof(accountName));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        }

        if (sharedKeyCredential == null)
        {
            throw new ArgumentNullException(nameof(sharedKeyCredential), "SharedKeyCredential cannot be null");
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options), "Table storage options cannot be null");
        }

        var validator = new TableStorageOptionsValidator();
        validator.ValidateAndThrow(options);

        var tableEndpoint = $"https://{accountName}.table.core.windows.net/";

        OptimisePerformance(new Uri(tableEndpoint), options);
        (_cloudTableService, CloudTable) = CreateTableClient(tableEndpoint, sharedKeyCredential, tableName, options.Retries, options.RetryWaitTimeInSeconds);

        _tableName = tableName;

        if (options.EnsureTableExists)
        {
            CreateTable();
        }
    }

    #endregion Table SharedKey Credential Construction

    /// <summary>
    /// Settings to improve performance
    /// </summary>
    private static void OptimisePerformance(Uri endpoint, TableStorageOptions options)
    {
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
                Mode = RetryMode.Exponential
            }
        };

        var serviceClient = new TableServiceClient(connectionString, options);
        var tableClient = serviceClient.GetTableClient(tableName);
        return (serviceClient, tableClient);
    }

    /// <summary>
    /// Create the table client
    /// </summary>
    /// <param name="tableEndpoint">The table endpoint</param>
    /// <param name="tokenCredential">The connection using token credentials</param>
    /// <param name="tableName">The name of the table</param>
    /// <param name="retries">Number of retries</param>
    /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
    /// <returns>The table client</returns>
    private static (TableServiceClient serviceClient, TableClient tableClient) CreateTableClient(string tableEndpoint, TokenCredential tokenCredential, string tableName, int retries, double retryWaitTimeInSeconds)
    {
        var options = new TableClientOptions
        {
            Retry =
            {
                MaxRetries = retries,
                Delay = TimeSpan.FromSeconds(retryWaitTimeInSeconds),
                Mode = RetryMode.Exponential
            }
        };

        var serviceClient = new TableServiceClient(new Uri(tableEndpoint), tokenCredential, options);
        var tableClient = serviceClient.GetTableClient(tableName);
        return (serviceClient, tableClient);
    }

    /// <summary>
    /// Create the table client
    /// </summary>
    /// <param name="tableEndpoint">The table endpoint</param>
    /// <param name="sasCredential">The connection using sas credentials</param>
    /// <param name="tableName">The name of the table</param>
    /// <param name="retries">Number of retries</param>
    /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
    /// <returns>The table client</returns>
    private static (TableServiceClient serviceClient, TableClient tableClient) CreateTableClient(string tableEndpoint, AzureSasCredential sasCredential, string tableName, int retries, double retryWaitTimeInSeconds)
    {
        var options = new TableClientOptions
        {
            Retry =
            {
                MaxRetries = retries,
                Delay = TimeSpan.FromSeconds(retryWaitTimeInSeconds),
                Mode = RetryMode.Exponential
            }
        };

        var serviceClient = new TableServiceClient(new Uri(tableEndpoint), sasCredential, options);
        var tableClient = serviceClient.GetTableClient(tableName);
        return (serviceClient, tableClient);
    }

    /// <summary>
    /// Create the table client
    /// </summary>
    /// <param name="tableEndpoint">The table endpoint</param>
    /// <param name="sharedKeyCredential">The connection using shared key credentials</param>
    /// <param name="tableName">The name of the table</param>
    /// <param name="retries">Number of retries</param>
    /// <param name="retryWaitTimeInSeconds">Wait time between retries in seconds</param>
    /// <returns>The table client</returns>
    private static (TableServiceClient serviceClient, TableClient tableClient) CreateTableClient(string tableEndpoint, TableSharedKeyCredential sharedKeyCredential, string tableName, int retries, double retryWaitTimeInSeconds)
    {
        var options = new TableClientOptions
        {
            Retry =
            {
                MaxRetries = retries,
                Delay = TimeSpan.FromSeconds(retryWaitTimeInSeconds),
                Mode = RetryMode.Exponential
            }
        };

        var serviceClient = new TableServiceClient(new Uri(tableEndpoint), sharedKeyCredential, options);
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
    /// <param name="cancellationToken">Used to cancel the operation</param>
    public Task CreateTableAsync(CancellationToken cancellationToken = default)
    {
        return CloudTable.CreateIfNotExistsAsync(cancellationToken);
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
    /// <param name="cancellationToken">Used to cancel the operation</param>
    /// <returns></returns>
    public async Task<bool> TableExistsAsync(CancellationToken cancellationToken = default)
    {
        return (await _cloudTableService.QueryAsync(e => e.Name == _tableName, cancellationToken: cancellationToken).ToListAsync(cancellationToken)).Any();
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
    /// <param name="cancellationToken">Used to cancel the operation</param>
    public Task DeleteTableAsync(CancellationToken cancellationToken = default)
    {
        return CloudTable.DeleteAsync(cancellationToken);
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
    /// <param name="cancellationToken">Used to cancel the operation</param>
    /// <returns>The record count</returns>
    public async Task<int> GetRecordCountAsync(CancellationToken cancellationToken = default)
    {
        return await (CloudTable.QueryAsync<TableEntity>(select: new List<string> { "PartitionKey" }, cancellationToken: cancellationToken)).CountAsync(cancellationToken);
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

    /// <summary>
    /// Ensures the record is not null.
    /// </summary>
    /// <param name="rowKey">The row key.</param>
    /// <exception cref="ArgumentNullException">rowKey</exception>
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
    protected Pageable<T> BuildGetByPartitionQuery<T>(string partitionKey) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.Query<T>(filter: $"PartitionKey eq '{partitionKey}'");
        return queryResults;
    }

    /// <summary>
    /// Builds the get by partition query.
    /// </summary>
    /// <param name="partitionKey">The partition key.</param>
    /// <param name="cancellationToken">Used to cancel the operation</param>
    /// <returns>The table query</returns>
    protected AsyncPageable<T> BuildGetByPartitionQueryAsync<T>(string partitionKey, CancellationToken cancellationToken) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.QueryAsync<T>(filter: $"PartitionKey eq '{partitionKey}'", cancellationToken: cancellationToken);
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

    /// <summary>
    /// Build the row key table query
    /// </summary>
    /// <param name="rowKey">The row key</param>
    /// <param name="cancellationToken">Used to cancel the operation</param>
    /// <returns>The table query</returns>
    protected AsyncPageable<T> BuildGetByRowKeyQueryAsync<T>(string rowKey, CancellationToken cancellationToken) where T : class, ITableEntity, new()
    {
        var queryResults = CloudTable.QueryAsync<T>(filter: $"RowKey eq '{rowKey}'", cancellationToken: cancellationToken);
        return queryResults;
    }

    #endregion Helpers
}