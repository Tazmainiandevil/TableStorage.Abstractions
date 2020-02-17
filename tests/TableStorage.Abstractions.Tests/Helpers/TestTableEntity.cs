using Microsoft.Azure.Cosmos.Table;

namespace TableStorage.Abstractions.Tests.Helpers
{
    public class TestTableEntity : TableEntity
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
    }
}