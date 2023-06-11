using FluentAssertions;
using FluentAssertions.Execution;
using System;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreTests
    {
        [Fact]
        public void delete_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.Delete(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public async Task delete_an_entry_and_the_record_count_should_decrease()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var item = _tableStorage.GetRecord("Smith", "John");

            // Act

            _tableStorage.Delete(item);

            var result = _tableStorage.GetByPartitionKey("Smith");

            // Assert
            result.Count().Should().Be(1);
        }

        [Fact]
        public void delete_using_wild_card_etag_when_entity_is_null_then_throws_an_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.DeleteUsingWildcardEtag(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public void delete_async_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.DeleteAsync(null as TestTableEntity);

            // Assert
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public async Task delete_async_an_entry_and_the_record_count_should_decrease()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var item = await _tableStorage.GetRecordAsync("Smith", "John");

            // Act
            await _tableStorage.DeleteAsync(item);

            var result = await _tableStorage.GetByPartitionKeyAsync("Smith");

            // Assert
            result.Count().Should().Be(1);
        }

        [Fact]
        public async Task delete_all_leaves_no_records_in_the_table()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            _tableStorage.DeleteAll();

            //
            _tableStorage.GetRecordCount().Should().Be(0);
        }

        [Fact]
        public async Task delete_all_when_over_100_leaves_no_records_in_the_table()
        {
            // Arrange
            await TestDataHelper.SetupDummyRecords(_tableStorage, 105);

            // Act
            _tableStorage.DeleteAll();

            //
            _tableStorage.GetRecordCount().Should().Be(0);
        }


        [Fact]
        public async Task delete_all_async_leaves_no_records_in_the_table()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            await _tableStorage.DeleteAllAsync();

            //
            (await _tableStorage.GetRecordCountAsync()).Should().Be(0);
        }

        [Fact]
        public async Task delete_all_async_when_over_100_leaves_no_records_in_the_table()
        {
            // Arrange
            await TestDataHelper.SetupDummyRecords(_tableStorage, 105);

            // Act
            await _tableStorage.DeleteAllAsync();

            //
            (await _tableStorage.GetRecordCountAsync()).Should().Be(0);
        }

        [Fact]
        public async Task delete_all_records_and_the_record_count_should_be_zero()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            await _tableStorage.DeleteAllAsync();
            var result = await _tableStorage.GetAllRecordsAsync();

            // Assert
            result.Count().Should().Be(0);
        }


        [Fact]
        public async Task delete_records_by_partitionkey_and_record_count_by_partition_should_be_zero()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            await _tableStorage.DeleteByPartitionAsync("Smith");
            var resultEmpty = await _tableStorage.GetByPartitionKeyAsync("Smith");
            var resultNotEmpty = await _tableStorage.GetByPartitionKeyAsync("Jones");

            // Assert
            using (new AssertionScope())
            {
                resultEmpty.Count().Should().Be(0);
                resultNotEmpty.Count().Should().NotBe(0);
            }
        }

        [Fact]
        public async Task delete_records_by_partitionkey_and_record_count_is_over_100_and_then_partition_should_be_zero()
        {
            // Arrange
            await TestDataHelper.SetupRecordsWithMoreThanMaxPartitionSize(_tableStorage);

            // Act
            await _tableStorage.DeleteByPartitionAsync("Smith");
            var resultEmpty = await _tableStorage.GetByPartitionKeyAsync("Smith");
            var resultNotEmpty = await _tableStorage.GetByPartitionKeyAsync("Jones");

            // Assert
            using (new AssertionScope())
            {
                resultEmpty.Count().Should().Be(0);
                resultNotEmpty.Count().Should().NotBe(0);
            }
        }
    }
}