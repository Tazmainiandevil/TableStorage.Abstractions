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

        //[Fact]
        //public async Task delete_all_records_and_the_record_count_should_be_zero()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorageDynamic);

        //    // Act
        //    await _tableStorageDynamic.DeleteAllAsync();
        //    var result = await _tableStorageDynamic.GetAllRecordsAsync();

        //    // Assert
        //    result.Count().Should().Be(0);
        //}

        //[Fact]
        //public async Task delete_records_by_partitionkey_and_record_count_by_partition_should_be_zero()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorageDynamic);

        //    // Act
        //    await _tableStorageDynamic.DeleteByPartitionAsync("Smith");
        //    var resultEmpty = await _tableStorageDynamic.GetByPartitionKeyAsync("Smith");
        //    var resultNotEmpty = await _tableStorageDynamic.GetByPartitionKeyAsync("Jones");

        //    // Assert
        //    resultEmpty.Count().Should().Be(0);
        //    resultNotEmpty.Count().Should().NotBe(0);
        //}

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
