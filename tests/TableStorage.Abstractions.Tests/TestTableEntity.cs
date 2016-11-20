using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorage.Abstractions.Tests
{
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
}