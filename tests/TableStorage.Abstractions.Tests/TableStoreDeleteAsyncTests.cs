using FluentAssertions;
using System;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreAsyncTests
    {
        [Fact]
        public void delete_async_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await tableStorage.DeleteAsync(null as TestTableEntity);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public async Task delete_async_an_entry_and_the_record_count_should_decrease()
        {
            // Arrange
            TestDataHelper.SetupRecords(tableStorage);
            var item = await tableStorage.GetRecordAsync("Smith", "John");

            // Act
            await tableStorage.DeleteAsync(item);

            var result = await tableStorage.GetByPartitionKeyAsync("Smith");

            // Assert
            result.Count().Should().Be(1);
        }
    }
}