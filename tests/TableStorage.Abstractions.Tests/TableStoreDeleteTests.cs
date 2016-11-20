using FluentAssertions;
using System;
using System.Linq;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreTests
    {
        [Fact]
        public void delete_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => tableStorage.Delete(null as TestTableEntity);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public void delete_an_entry_and_the_record_count_should_decrease()
        {
            // Arrange
            TestDataHelper.SetupRecords(tableStorage);
            var item = tableStorage.GetRecord("Smith", "John");

            // Act

            tableStorage.Delete(item);

            var result = tableStorage.GetByPartitionKey("Smith");

            // Assert
            result.Count().Should().Be(1);
        }
    }
}