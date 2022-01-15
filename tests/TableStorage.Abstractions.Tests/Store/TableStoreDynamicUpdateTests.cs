using FluentAssertions;
using System;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreDynamicTests
    {
        [Fact]
        public void update_dynamic_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorageDynamic.Update(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public void update_async_dynamic_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorageDynamic.UpdateAsync(null as TestTableEntity);

            // Assert
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public async Task update_a_dynamic_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorageDynamic);

            // Act
            var item = _tableStorageDynamic.GetRecord<TestTableEntity>("Smith", "John");

            item.Age = 22;

            _tableStorageDynamic.Update(item);

            var item2 = _tableStorageDynamic.GetRecord<TestTableEntity>("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }

        [Fact]
        public async Task update_async_a_dynamic_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorageDynamic);

            // Act
            var item = await _tableStorageDynamic.GetRecordAsync<TestTableEntity>("Smith", "John");

            item.Age = 22;

            await _tableStorageDynamic.UpdateAsync(item);

            var item2 = await _tableStorageDynamic.GetRecordAsync<TestTableEntity>("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }
    }
}