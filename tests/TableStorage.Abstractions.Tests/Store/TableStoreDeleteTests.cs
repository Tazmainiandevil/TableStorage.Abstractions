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
    }
}