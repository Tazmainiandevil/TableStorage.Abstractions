using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreTests
    {
        [Fact]
        public void insert_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => tableStorage.Insert(null as TestTableEntity);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public void insert_record_into_the_table_and_record_count_should_be_greater_than_zero()
        {
            // Arrange
            var testEntity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };

            // Act
            tableStorage.Insert(testEntity);

            var result = tableStorage.GetByRowKey("John").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public void insert_with_null_for_multiple_records_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => tableStorage.Insert(null as IEnumerable<TestTableEntity>);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: records");
        }

        [Fact]
        public void insert_multiple_records_into_the_table_and_record_count_should_be_greater_than_zero()
        {
            // Arrange
            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
            };

            // Act
            tableStorage.Insert(entityList);
            var result = tableStorage.GetByPartitionKey("Smith").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public void insert_with_empty_list_of_records_does_not_insert_records_to_the_table()
        {
            // Arrange
            TestDataHelper.SetupRecords(tableStorage);

            var extraList = new List<TestTableEntity>();

            // Act
            tableStorage.Insert(extraList);
            var result = tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(4);
        }

        [Fact]
        public void insert_multiple_records_with_different_partition_keys_inserts_the_expected_count()
        {
            // Arrange
            var entryList = TestDataHelper.GetMultiplePartitionKeyRecords();


            // Act
            tableStorage.Insert(entryList);
            var result = tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(entryList.Count);
        }
    }
}