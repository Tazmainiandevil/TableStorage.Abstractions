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
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_record_async_with_null_or_empty_partition_key_throws_exception(string partitionKey)
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.GetRecordAsync(partitionKey, "someRowKey");

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_record_async_with_null_or_empty_row_key_throws_exception(string rowKey)
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.GetRecordAsync("somePartitionKey", rowKey);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: rowKey");
        }

        [Fact]
        public async Task get_record_async_with_no_entry_returns_null()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = await _tableStorage.GetRecordAsync("surname", "first");

            // Assert
            result.Should().BeNull();
        }

        [Fact]
        public async Task get_record_async_with_an_entry_returns_the_expected_entry()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);
            var expected = new TestTableEntity("Bill", "Jones") { Age = 45, Email = "bill.jones@somewhere.com" };

            // Act
            var result = await _tableStorage.GetRecordAsync("Jones", "Bill");

            // Assert
            result.ShouldBeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath == "CompiledRead"));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_records_by_partition_key_async_with_null_or_empty_value_throws_exception(string partitionKey)
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.GetByPartitionKeyAsync(partitionKey);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
        }

        [Fact]
        public async Task get_records_by_partition_key_async_with_unknown_key_returns_empty_list()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);
            var partitionKey = "something";

            // Act
            var result = await _tableStorage.GetByPartitionKeyAsync(partitionKey);

            // Assert
            result.ShouldAllBeEquivalentTo(new List<TestTableEntity>());
        }

        public static IEnumerable<object[]> PartitionExpectedData
        {
            get
            {
                yield return new object[]
                {
                    "Smith", new List<TestTableEntity>
                    {
                        new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                        new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
                    }
                };
                yield return new object[]
                {
                    "Jones", new List<TestTableEntity>
                    {
                        new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                        new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
                    }
                };
            }
        }

        [Theory]
        [MemberData("PartitionExpectedData")]
        public async Task get_records_by_partition_key_async_with_known_key_returns_the_expected_results(string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetByPartitionKeyAsync(partitionKey);

            // Assert
            results.ShouldAllBeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_records_by_row_key_async_with_null_or_empty_value_throws_exception(string rowKey)
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.GetByRowKeyAsync(rowKey);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: rowKey");
        }

        [Fact]
        public async Task get_records_by_row_key_async_with_unknown_key_returns_empty_list()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);
            var rowKey = "something";

            // Act
            var result = await _tableStorage.GetByRowKeyAsync(rowKey);

            // Assert
            result.ShouldAllBeEquivalentTo(new List<TestTableEntity>());
        }

        public static IEnumerable<object[]> RowKeyExpectedData
        {
            get
            {
                yield return new object[]
                {
                    "Bill", new List<TestTableEntity>
                    {
                        new TestTableEntity("Bill", "Smith") {Age = 38, Email = "bill.smith@another.com"},
                        new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"},
                        new TestTableEntity("Bill", "King") {Age = 45, Email = "bill.king@email.com"}
                    }
                };
                yield return new object[]
                {
                    "Fred", new List<TestTableEntity>
                    {
                        new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                        new TestTableEntity("Fred", "Bloggs") {Age = 32, Email = "fred.bloggs@email.com"}
                    }
                };
            }
        }

        [Theory]
        [MemberData("RowKeyExpectedData")]
        public async Task get_records_by_row_key_async_with_known_key_returns_the_expected_results(string rowKey, List<TestTableEntity> expected)
        {
            // Arrange
            TestDataHelper.SetupRowKeyRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetByRowKeyAsync(rowKey);

            // Assert
            results.ShouldAllBeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        }

        [Fact]
        public async Task get_all_records_async_with_an_empty_table_returns_an_empty_list()
        {
            // Arrange
            // Act
            var results = await _tableStorage.GetAllRecordsAsync();

            // Assert
            results.Should().BeEmpty();
        }

        [Fact]
        public async Task get_all_records_async_with_entries_returns_the_expected_count()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetAllRecordsAsync();

            // Assert
            results.Count().Should().Be(4);
        }

        [Fact]
        public async Task get_record_count_async_with_entries_returns_the_expected_count()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = await _tableStorage.GetRecordCountAsync();

            // Assert
            result.Should().Be(4);
        }
    }
}