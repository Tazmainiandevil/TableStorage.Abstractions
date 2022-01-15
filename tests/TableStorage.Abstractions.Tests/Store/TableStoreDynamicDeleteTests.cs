using FluentAssertions;
using System;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreDynamicTests
    {
        [Fact]
        public void delete_dynamic_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorageDynamic.Delete(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public async Task delete_a_dynamic_entry_and_the_record_count_should_decrease()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorageDynamic);
            var item = _tableStorageDynamic.GetRecord<TestTableEntity>("Smith", "John");

            // Act

            _tableStorageDynamic.Delete(item);

            var result = _tableStorageDynamic.GetByPartitionKey<TestTableEntity>("Smith");

            // Assert
            result.Count().Should().Be(1);
        }

        [Fact]
        public void delete_async_dynamic_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorageDynamic.DeleteAsync(null as TestTableEntity);

            // Assert
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public async Task delete_async_a_dynamic_entry_and_the_record_count_should_decrease()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorageDynamic);
            var item = await _tableStorageDynamic.GetRecordAsync<TestTableEntity>("Smith", "John");

            // Act
            await _tableStorageDynamic.DeleteAsync(item);

            var result = await _tableStorageDynamic.GetByPartitionKeyAsync<TestTableEntity>("Smith");

            // Assert
            result.Count().Should().Be(1);
        }
    }
}