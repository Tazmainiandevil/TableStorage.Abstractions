using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreAsyncTests
    {
        [Fact]
        public void update_async_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await tableStorage.UpdateAsync(null as TestTableEntity);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public async Task update_async_a_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            TestDataHelper.SetupRecords(tableStorage);

            // Act
            var item = await tableStorage.GetRecordAsync("Smith", "John");

            item.Age = 22;

            await tableStorage.UpdateAsync(item);

            var item2 = await tableStorage.GetRecordAsync("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }
    }
}
