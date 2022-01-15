using FluentAssertions;
using System;
using System.Threading.Tasks;
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
            Action act = () => _tableStorage.Update(null);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Record cannot be null*");
        }


        [Fact]
        public async Task update_a_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

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
            Action act = () => _tableStorage.UpdateUsingWildcardEtag(null);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public async Task update_using_wildcard_etag_the_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var item = _tableStorage.GetRecord("Smith", "John");

            item.Age = 22;

            _tableStorage.UpdateUsingWildcardEtag(item);

            var item2 = _tableStorage.GetRecord("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }

        [Fact]
        public void update_async_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.UpdateAsync(null);

            // Assert
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Record cannot be null*");
        }

        [Fact]
        public async Task update_async_a_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var item = await _tableStorage.GetRecordAsync("Smith", "John");

            item.Age = 22;

            await _tableStorage.UpdateAsync(item);

            var item2 = await _tableStorage.GetRecordAsync("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }

        [Fact]
        public void update_using_wildcard_etag_async_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.UpdateUsingWildcardEtagAsync(null);

            // Assert
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public async Task update_using_wildcard_etag_async_the_record_in_the_table_and_the_change_should_be_recorded()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var item = await _tableStorage.GetRecordAsync("Smith", "John");

            item.Age = 22;

            await _tableStorage.UpdateUsingWildcardEtagAsync(item);

            var item2 = await _tableStorage.GetRecordAsync("Smith", "John");

            // Assert
            item2.Age.Should().Be(22);
        }
    }
}