using FluentAssertions;
using System;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreTests
    {
        [Fact]
        public void update_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => tableStorage.Update(null as TestTableEntity);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public void update_a_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            TestDataHelper.SetupRecords(tableStorage);

            // Act
            var item = tableStorage.GetRecord("Smith", "John");

            item.Age = 22;

            tableStorage.Update(item);

            var item2 = tableStorage.GetRecord("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }
    }
}