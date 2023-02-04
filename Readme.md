# TableStorage.Abstractions

Repository wrapper for Azure Table Storage in C# using the Azure.Data.Tables libraries and supporting .NET Standard 2.0 and tested in .NET Framework 4.8 and .NET 6.0

<image src="https://ci.appveyor.com/api/projects/status/github/Tazmainiandevil/TableStorage.Abstractions?branch=master&svg=true">
<a href="https://badge.fury.io/nu/TableStorage.Abstractions"><img src="https://badge.fury.io/nu/TableStorage.Abstractions.svg" alt="NuGet version" height="18"></a>

![Known Vulnerabilities](https://snyk.io/test/github/tazmainiandevil/tablestorage.abstractions/badge.svg)

## Introduction

Working with Azure Table Storage has been interesting and very different from working with SQL Server which I have done for many years. After reading a number of articles about it and using it I realised a generic wrapper would be useful to aid unit testing and so this is the result of that realisation.

I referenced a number of articles on Table Storage most of which are quite old now but still valid. Suggestions from these articles have been included in this library.

<https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/>

<https://azure.microsoft.com/en-gb/blog/managing-concurrency-in-microsoft-azure-storage-2/>

<https://docs.microsoft.com/en-us/azure/storage/storage-table-design-guide>

<https://docs.particular.net/nservicebus/azure-storage-persistence/performance-tuning>

<http://robertgreiner.com/2012/06/why-is-azure-table-storage-so-slow/>

## Configuration

Optimisations are controlled by the Table Storage Options Class.
The defaults are applied as below if not overridden:

```C#
public class TableStorageOptions
{
    public bool UseNagleAlgorithm { get; set; } = false;

    public bool Expect100Continue { get; set; } = false;

    public int ConnectionLimit { get; set; } = 10;

    public int Retries { get; set; } = 3;

    public double RetryWaitTimeInSeconds { get; set; } = 1;

    public bool EnsureTableExists { get; set; } = true;
}
```

Example entity:

__NOTE__: Azure.Data.Tables requires inheritance from the interface as the base class TableEntity is a sealed class.

```C#
public class TestTableEntity : ITableEntity
{
    public string PartitionKey { get; set; }
    public string RowKey { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public int Age { get; set; }
    public string Email { get; set; }

    public TestTableEntity() {}

    public TestTableEntity(string name, string surname)
    {
        PartitionKey = surname;
        RowKey = name;
    }
}

```

The library also includes a factory class to make it easier when using dependency injection with multiple tables. This can create a table store with the default TableStorageOptions which is used when not specified, or override the options depending on your needs.

```C#
public class TestTableStorageClient
{
    private ITableStore<MyStuff> _store;

    public TestTableStorageClient(ITableStoreFactory factory)
    {
        _store = factory.CreateTableStore<MyStuff>("MyTable", "UseDevelopmentStorage=true");
    }
}
```

Override TableStorageOptions when using the factory creation

```C#
public class TestTableStorageClient
{
    private ITableStore<MyStuff> _store;

    public TestTableStorageClient(ITableStoreFactory factory)
    {
        var options = new TableStorageOptions
        {
            UseNagleAlgorithm = true,
            ConnectionLimit = 100,
            EnsureTableExists = false
        };

        _store = factory.CreateTableStore<MyStuff>("MyTable", "UseDevelopmentStorage=true", options);
    }
}
```

Override TableStorageOptions when using the store creation

```C#
public class TestTableStorageClient
{
    private ITableStore<MyStuff> _store;

    public TestTableStorageClient()
    {
        var options = new TableStorageOptions
        {
            UseNagleAlgorithm = true,
            ConnectionLimit = 100,
            EnsureTableExists = false
        };

        _store = new TableStore<MyStuff>("MyTable", "UseDevelopmentStorage=true", options);
    }
}
```

### Insert Records

Example Insert of a record

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var entity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };

await tableStorage.InsertAsync(entity);

// Get the entries by the row key
var result = tableStorage.GetByRowKey("John").ToList();
```

Inserting multiple entries into table storage requires each entry to have the same partition key for a batch. This implementation in the wrapper does this job for you so that you can just pass a list of entities.

Example Insert of multiple records

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var entries = new List<TestTableEntity>
{
    new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
    new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"},
    new TestTableEntity("Bill", "Smith") { Age = 38, Email = "bill.smith@another.com"},
    new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
    new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"},
    new TestTableEntity("Bill", "King") {Age = 45, Email = "bill.king@email.com"},
    new TestTableEntity("Fred", "Bloggs") { Age = 32, Email = "fred.bloggs@email.com" }
};

await tableStorage.InsertAsync(entries);
```

### InsertOrReplace

Example of Insert or Replace of a record

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var entity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };

await tableStorage.InsertOrReplaceAsync(entity);
```

### Update Records

Example of Updating a record
```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");

// Get the current record
var entity = await tableStorage.GetRecordAsync("Smith", "John");

// Update properties
entity.Age = 22;

await tableStorage.UpdateAsync(entity);
```

### Delete Records

Example of deleting a record

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");

// Get the current record
var entity = await tableStorage.GetRecordAsync("Smith", "John");

await tableStorage.DeleteAsync(entity);
```

Example of deleting all records for a partition

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");

await tableStorage.DeleteByPartitionAsync("Smith");
```

Example of deleting all records in all partitions

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");

await tableStorage.DeleteAllAsync();
```

### Filter Records

Table Storage does not really have generic way of filtering data as yet. So there are some methods to help with that.
NOTE: The filtering works by getting all records so on large datasets this will be slow.
Testing showed ~1.3 seconds for 10,000 records
Testing when paged by 100 ~0.0300 seconds for 10,000 records returning 100 records

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var results = tableStorage.GetRecordsByFilter(x => x.Age > 21 && x.Age < 25);
```

And with basic paging starting at 0 and returning 100
NOTE: The start is number of records e.g. 20, 100 would start at record 20 and then return a maxiumum of 100 after that

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var results = tableStorage.GetRecordsByFilter(x => x.Age > 21 && x.Age < 25, 0, 100);
```

There is also the consideration of using Reactive Extensions (RX - <http://reactivex.io/>) to observe the results from a get all records call or a get filtered records.

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var theObserver = tableStorage.GetAllRecordsObservable();
theObserver.Where(x => x.Age > 21 && x.Age < 25).Take(100).Subscribe(x =>
{
   // Do something with the table entry
});
```

or

```C#
var tableStorage = new TableStore<TestTableEntity>("MyTable", "UseDevelopmentStorage=true");
var theObserver = tableStorage.GetRecordsByFilterObservable(x => x.Age > 21 && x.Age < 25, 0, 100);
theObserver.Subscribe(x =>
{
   // Do something with the table entry
});
```

## Heterogeneous Tables

As Table storage is a schema-less store there are times when you are dealing with multiple entity types in a single table.
<https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-design-guide#work-with-heterogeneous-entity-types>

This library has some additional support for those times.

When creating a TableStore if no generic type is supplied then it creates a dynamic store. This allows the basic methods, Insert, Update, GetRecord, etc. to specify the generic type on the method call. Getting all records now returns a list of DynamicTableEntity but you can still get by partition key using a generic type.

```C#
var tableStorage = new TableStoreDynamic("MyTable", "UseDevelopmentStorage=true");
var entity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };
var otherEntity = new AnotherTableEntity("52a54878-b4b3-45bd-bc5b-3822989b460f", "MyProduct") { Name = "Product", Url = "https://someendpoint" };

await tableStorage.InsertAsync<TestTableEntity>(entity);
await tableStorage.InsertAsync<AnotherTableEntity>(otherEntity);

var employee = await GetRecordAsync<TestTableEntity>("Smith", "John");
var productEntity = await GetRecordAsync<otherEntity>("MyProduct", "52a54878-b4b3-45bd-bc5b-3822989b460f");
```

__NOTE__: Currently only the basic methods are supported for this type of table, there are no filter/search methods.

## Useful Reading

<https://docs.microsoft.com/en-gb/azure/storage/storage-dotnet-how-to-use-tables>
<http://www.introtorx.com/content/v1.0.10621.0/01_WhyRx.html>

## Notes

Most methods have a synchronous and asynchronous version.

The unit tests rely on using Azurite Emulator which is now bundled with Visual Studio 2022 details can be found on [Microsoft Docs](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio) for other installations including Visual Studio Code and Docker.
