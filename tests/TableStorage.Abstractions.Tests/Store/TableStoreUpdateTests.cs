using System;
using FluentAssertions;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreTests
    {
        [Fact]
        public void update_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.Update(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public void update_a_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var item = _tableStorage.GetRecord("Smith", "John");

            item.Age = 22;

            _tableStorage.Update(item);

            var item2 = _tableStorage.GetRecord("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }

        [Fact]
        public void update_using_wildcard_etag_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.UpdateUsingWildcardEtag(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public void update_using_wildcard_etag_the_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var item = _tableStorage.GetRecord("Smith", "John");

            item.Age = 22;

            _tableStorage.UpdateUsingWildcardEtag(item);

            var item2 = _tableStorage.GetRecord("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }
    }
}