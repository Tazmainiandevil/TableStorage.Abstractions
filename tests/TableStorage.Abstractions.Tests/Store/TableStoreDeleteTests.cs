using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreTests
    {
#if !NETCOREAPP2_0 && !NETCOREAPP2_1
        [Fact]
        public void delete_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.Delete(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
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
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }
        #endif
    }
}