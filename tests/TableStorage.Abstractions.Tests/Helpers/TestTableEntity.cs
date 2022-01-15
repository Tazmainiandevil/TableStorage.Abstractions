using Azure;
using Azure.Data.Tables;
using System;

namespace TableStorage.Abstractions.Tests.Helpers
{
    public class TestTableEntity : ITableEntity
    {
        public int Age { get; set; }
        public string Email { get; set; }

        public TestTableEntity()
        {
        }

        public TestTableEntity(string name, string surname)
        {
            PartitionKey = surname;
            RowKey = name;
        }

        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}