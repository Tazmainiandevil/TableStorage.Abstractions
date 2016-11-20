using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreAsyncTests
    {
        [Fact]
        public void insert_async_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await tableStorage.InsertAsync(null as TestTableEntity);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public async Task insert_record_into_the_table_async_inserts_with_a_count_greater_than_zero()
        {
            // Arrange
            var testEntity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };

            // Act
            await tableStorage.InsertAsync(testEntity);
            var result = tableStorage.GetByRowKey("John").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public void insert_async_with_null_for_multiple_records_throws_exception()
        {
            // Arrange
            // Act
            Func<Task> act = async () => await tableStorage.InsertAsync(null as IEnumerable<TestTableEntity>);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: records");
        }

        [Fact]
        public async Task insert_async_multiple_records_into_the_table_and_record_count_should_be_greater_than_zero()
        {
            // Arrange
            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
            };

            // Act
            await tableStorage.InsertAsync(entityList);
            var result = tableStorage.GetByPartitionKey("Smith").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task insert_async_with_empty_list_of_records_does_not_insert_records_to_the_table()
        {
            // Arrange
            TestDataHelper.SetupRecords(tableStorage);

            var extraList = new List<TestTableEntity>();

            // Act
            await tableStorage.InsertAsync(extraList);
            var result = tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(4);
        }

        [Fact]
        public async Task insert_async_multiple_records_with_different_partition_keys_inserts_the_expected_count()
        {
            // Arrange
            var entryList = TestDataHelper.GetMultiplePartitionKeyRecords();


            // Act
            await tableStorage.InsertAsync(entryList);
            var result = await tableStorage.GetAllRecordsAsync();

            // Assert
            result.Count().Should().Be(entryList.Count);
        }
    }
}