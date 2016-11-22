# TableStorage.Abstractions
Repository wrapper for Azure Table Storage in C# .NET 4.6

<image src="https://ci.appveyor.com/api/projects/status/github/Tazmainiandevil/TableStorage.Abstractions?branch=master&svg=true">
[![NuGet version](https://badge.fury.io/nu/TableStorage.Abstractions.svg)](https://badge.fury.io/nu/TableStorage.Abstractions)

Starting work with Azure Table Storage has been interesting and very different from working with SQL Server which I have done for many years. After reading a number of articles about it and using it I realised a generic wrapper would be useful to create and so this is that creation.

Example entity:
```C#
public class TestTableEntity : TableEntity
{
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
Example usage:
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

__Useful Reading__

https://docs.microsoft.com/en-gb/azure/storage/storage-dotnet-how-to-use-tables

__Notes__

Each method has a synchronous and asynchronous version.

The unit tests rely on using Azure Storage Emulator (which can be found here https://azure.microsoft.com/en-gb/downloads/).
