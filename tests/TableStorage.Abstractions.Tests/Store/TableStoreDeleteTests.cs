using FluentAssertions;
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
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        //[Fact]
        //public void delete_dynamic_with_null_record_throws_exception()
        //{
        //    // Arrange
        //    // Act
        //    Action act = () => _tableStorageDynamic.Delete(null as TestTableEntity);

        //    // Assert
        //    act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        //}

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


        //[Fact]
        //public async Task delete_a_dynamic_entry_and_the_record_count_should_decrease()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorageDynamic);
        //    var item = _tableStorageDynamic.GetRecord<TestTableEntity>("Smith", "John");

        //    // Act

        //    _tableStorageDynamic.Delete(item);

        //    var result = _tableStorageDynamic.GetByPartitionKey<TestTableEntity>("Smith");

        //    // Assert
        //    result.Count().Should().Be(1);
        //}

        [Fact]
        public void delete_using_wild_card_etag_when_entity_is_null_then_throws_an_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.DeleteUsingWildcardEtag(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        //[Fact]
        //public async Task delete_all_records_and_the_record_count_should_be_zero()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorageDynamic);

        //    // Act
        //    await _tableStorage.DeleteAllAsync();
        //    var result = await _tableStorage.GetAllRecordsAsync();

        //    // Assert
        //    result.Count().Should().Be(0);
        //}

        //[Fact]
        //public async Task delete_records_by_partitionkey_and_record_count_by_partition_should_be_zero()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorageDynamic);

        //    // Act
        //    await _tableStorage.DeleteByPartitionAsync("Smith");
        //    var resultEmpty = await _tableStorage.GetByPartitionKeyAsync("Smith");
        //    var resultNotEmpty = await _tableStorage.GetByPartitionKeyAsync("Jones");

        //    // Assert
        //    resultEmpty.Count().Should().Be(0);
        //    resultNotEmpty.Count().Should().NotBe(0);
        //}
    }
}