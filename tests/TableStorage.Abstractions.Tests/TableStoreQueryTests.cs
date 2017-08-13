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
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_record_with_null_or_empty_partition_key_throws_exception(string partitionKey)
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.GetRecord(partitionKey, "someRowKey");

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_record_with_null_or_empty_row_key_throws_exception(string rowKey)
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.GetRecord("somePartitionKey", rowKey);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: rowKey");
        }
      

        [Fact]
        public void get_record_with_no_entry_returns_null()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = _tableStorage.GetRecord("surname", "first");

            // Assert
            result.Should().BeNull();
        }

        [Fact]
        public void get_record_with_an_entry_returns_the_expected_entry()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);
            var expected = new TestTableEntity("Bill", "Jones") { Age = 45, Email = "bill.jones@somewhere.com" };

            // Act
            var result = _tableStorage.GetRecord("Jones", "Bill");

            // Assert
            result.ShouldBeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath == "CompiledRead"));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_records_by_partition_key_with_null_or_empty_value_throws_exception(string partitionKey)
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.GetByPartitionKey(partitionKey);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
        }

        [Fact]
        public void get_records_by_partition_key_with_unknown_key_returns_empty_list()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);
            var partitionKey = "something";

            // Act
            var result = _tableStorage.GetByPartitionKey(partitionKey);

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
        public void get_records_by_partition_key_with_known_key_returns_the_expected_results(string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByPartitionKey(partitionKey);

            // Assert
            results.ShouldAllBeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_records_by_row_key_with_null_or_empty_value_throws_exception(string rowKey)
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.GetByRowKey(rowKey);

            // Assert
            act.ShouldThrow<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: rowKey");
        }

        [Fact]
        public void get_records_by_row_key_with_unknown_key_returns_empty_list()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);
            var rowKey = "something";

            // Act
            var result = _tableStorage.GetByRowKey(rowKey);

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
        public void get_records_by_row_key_with_known_key_returns_the_expected_results(string rowKey, List<TestTableEntity> expected)
        {
            // Arrange            
            TestDataHelper.SetupRowKeyRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByRowKey(rowKey);

            // Assert
            results.ShouldAllBeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        }

        [Fact] public void get_all_records_with_an_empty_table_returns_an_empty_list()
        {
            // Arrange
            // Act
            var results = _tableStorage.GetAllRecords();

            // Assert
            results.Should().BeEmpty();
        }


        [Fact]
        public void get_all_records_with_entries_returns_the_expected_count()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetAllRecords();

            // Assert
            results.Count().Should().Be(4);
        }

        [Fact]
        public void get_record_count_with_entries_returns_the_expected_count()
        {
            // Arrange
            TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = _tableStorage.GetRecordCount();

            // Assert
            result.Should().Be(4);
        }
    }
}