using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreTests
    {
        [Fact]
        public void insert_with_null_record_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.Insert(null as TestTableEntity);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: record");
        }

        [Fact]
        public void insert_record_into_the_table_and_record_count_should_be_greater_than_zero()
        {
            // Arrange
            var testEntity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };

            // Act
            _tableStorage.Insert(testEntity);

            var result = _tableStorage.GetByRowKey("John").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public void insert_or_replace_record_into_the_table_when_record_does_not_exist_and_record_count_should_be_greater_than_zero()
        {
            // Arrange
            var testEntity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };

            // Act
            _tableStorage.InsertOrReplace(testEntity);

            var result = _tableStorage.GetByRowKey("John").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public void insert_or_replace_record_into_the_table_when_record_does_exist_and_record_should_have_updated_fields()
        {
            // Arrange
            var testEntity = new TestTableEntity("John", "Smith") { Age = 21, Email = "john.smith@something.com" };
            _tableStorage.Insert(testEntity);
            // Act
            testEntity = new TestTableEntity("John", "Smith") { Age = 45, Email = "john.smith@something.com" };
            _tableStorage.InsertOrReplace(testEntity);

            var result = _tableStorage.GetByRowKey("John").ToList();

            // Assert
            result[0].Age.Should().Be(45);
        }

        [Fact]
        public void insert_with_null_for_multiple_records_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.Insert(null as IEnumerable<TestTableEntity>);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: records");
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
            _tableStorage.Insert(entityList);
            var result = _tableStorage.GetByPartitionKey("Smith").ToList();

            // Assert
            result.Count.Should().BeGreaterThan(0);
        }

        [Fact]
        public async Task insert_with_empty_list_of_records_does_not_insert_records_to_the_table()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            _tableStorage.Insert(new List<TestTableEntity>());
            var result = _tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(4);
        }

        [Fact]
        public void insert_multiple_records_with_different_partition_keys_inserts_the_expected_count()
        {
            // Arrange
            var entryList = TestDataHelper.GetMultiplePartitionKeyRecords();

            // Act
            _tableStorage.Insert(entryList);
            var result = _tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(entryList.Count);
        }

        [Fact]
        public void insert_multiple_records_with_same_partition_key_and_more_than_the_100_max_batch_size_still_inserts_all_the_records()
        {
            // Arrange
            var entryList = TestDataHelper.GetMoreThanMaxSinglePartitionRecords();

            // Act
            _tableStorage.Insert(entryList);
            var result = _tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(entryList.Count);
        }

        [Fact]
        public void insert_multiple_records_with_multiple_partition_keys_and_more_than_the_100_max_batch_size_in_for_all_and_still_inserts_all_the_records()
        {
            // Arrange
            var entryList = TestDataHelper.GetMoreThanMaxMultiplePartitionRecords();

            // Act
            _tableStorage.Insert(entryList);
            var result = _tableStorage.GetAllRecords().ToList();

            // Assert
            result.Count.Should().Be(entryList.Count);
        }
    }
}