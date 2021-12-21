﻿using Azure;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
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
            act.Should().ThrowAsync<ArgumentNullException>()
                .WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
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
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: rowKey");
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
            act.Should().ThrowAsync<ArgumentNullException>()
                .WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
        }

        //[Theory]
        //[InlineData(null)]
        //[InlineData("")]
        //[InlineData("    ")]
        //public void get_records_by_partition_key_paged_async_with_null_or_empty_value_throws_exception(
        //    string partitionKey)
        //{
        //    // Arrange
        //    // Act
        //    Func<Task> act = async () => await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey);

        //    // Assert
        //    act.Should().Throw<ArgumentNullException>()
        //        .WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
        //}

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

        public static IEnumerable<object[]> PartitionExpectedDataPageOfOne
        {
            get
            {
                yield return new object[]
                {
                    "Smith", new List<TestTableEntity>
                    {
                        new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
                    }
                };
                yield return new object[]
                {
                    "Jones", new List<TestTableEntity>
                    {
                        new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
                    }
                };
            }
        }

        public static IEnumerable<object[]> PartitionExpectedDataPageOfOneNextPage
        {
            get
            {
                yield return new object[]
                {
                    "Smith", new List<TestTableEntity>
                    {
                        new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"}
                    }
                };
                yield return new object[]
                {
                    "Jones", new List<TestTableEntity>
                    {
                        new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"}
                    }
                };
            }
        }

        [Theory]
        [MemberData(nameof(PartitionExpectedData))]
        public async Task get_records_by_partition_key_async_with_known_key_returns_the_expected_results(
            string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetByPartitionKeyAsync(partitionKey);

            // Assert
            results.Should().BeEquivalentTo(expected,
                op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
                    .Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        //[Theory]
        //[MemberData(nameof(PartitionExpectedData))]
        //public async Task get_records_by_partition_key_paged_async_with_known_key_returns_the_expected_results(
        //    string partitionKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected,
        //        op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
        //            .Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[MemberData(nameof(PartitionExpectedDataPageOfOne))]
        //public async Task
        //    get_records_by_partition_key_paged_async_with_known_key_returns_the_expected_results_and_expected_row_count(
        //        string partitionKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey, 1);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected,
        //        op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
        //            .Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[MemberData(nameof(PartitionExpectedDataPageOfOneNextPage))]
        //public async Task
        //    get_records_by_partition_key_paged_async_with_known_key_second_page_returns_the_expected_results_and_expected_row_count(
        //        string partitionKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey, 1);
        //    results = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey, 1, results.ContinuationToken);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected,
        //        op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
        //            .Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[InlineData("Smith")]
        //[InlineData("Jones")]
        //public async Task
        //    get_records_by_partition_key_paged_async_with_known_key_returns_the_expected_results_with_final_page_annotated(
        //        string partitionKey)
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey, 1);
        //    results = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey, 1, results.ContinuationToken);

        //    // Assert
        //    results.IsFinalPage.Should().BeTrue();
        //}

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
            act.Should().ThrowAsync<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: rowKey");
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

        public static IEnumerable<object[]> RowKeyExpectedDataPageOfOne
        {
            get
            {
                yield return new object[]
                {
                    "Bill", new List<TestTableEntity>
                    {
                        new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
                    }
                };
                yield return new object[]
                {
                    "Fred", new List<TestTableEntity>
                    {
                        new TestTableEntity("Fred", "Bloggs") {Age = 32, Email = "fred.bloggs@email.com"}
                    }
                };
            }
        }

        public static IEnumerable<object[]> RowKeyExpectedDataPageOfOneNextPage
        {
            get
            {
                yield return new object[]
                {
                    "Bill", new List<TestTableEntity>
                    {
                        new TestTableEntity("Bill", "King") {Age = 45, Email = "bill.king@email.com"}
                    }
                };
                yield return new object[]
                {
                    "Fred", new List<TestTableEntity>
                    {
                        new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"}
                    }
                };
            }
        }

        [Theory]
        [MemberData(nameof(RowKeyExpectedData))]
        public async Task get_records_by_row_key_async_with_known_key_returns_the_expected_results(string rowKey,
            List<TestTableEntity> expected)
        {
            // Arrange
            TestDataHelper.SetupRowKeyRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetByRowKeyAsync(rowKey);

            // Assert
            results.Should().BeEquivalentTo(expected,
                op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
                    .Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        //[Theory]
        //[MemberData(nameof(RowKeyExpectedData))]
        //public async Task get_records_by_row_key_with_known_key_paged_async_returns_the_expected_results(string rowKey,
        //    List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    TestDataHelper.SetupRowKeyRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByRowKeyPagedAsync(rowKey);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected,
        //        op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
        //            .Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[MemberData(nameof(RowKeyExpectedDataPageOfOne))]
        //public async Task
        //    get_records_by_row_key_with_known_key_paged_async_returns_the_expected_results_and_expected_row_count(
        //        string rowKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    TestDataHelper.SetupRowKeyRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByRowKeyPagedAsync(rowKey, 1);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected,
        //        op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
        //            .Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[MemberData(nameof(RowKeyExpectedDataPageOfOneNextPage))]
        //public async Task
        //    get_records_by_row_key_with_known_key_paged_async_second_page_returns_the_expected_results_and_expected_row_count(
        //        string rowKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    TestDataHelper.SetupRowKeyRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetByRowKeyPagedAsync(rowKey, 1);
        //    results = await _tableStorage.GetByRowKeyPagedAsync(rowKey, 1, results.ContinuationToken);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected,
        //        op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
        //            .Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

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
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetAllRecordsAsync();

            // Assert
            results.Count().Should().Be(4);
        }

        //[Fact]
        //public async Task get_all_records_paged_async_with_an_empty_table_returns_an_empty_list()
        //{
        //    // Arrange
        //    // Act
        //    var results = await _tableStorage.GetAllRecordsPagedAsync();

        //    // Assert
        //    results.Items.Should().BeEmpty();
        //}

        //[Fact]
        //public async Task get_all_records_with_entries_paged_async_does_not_repeat_results_when_paging()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results1 = await _tableStorage.GetAllRecordsPagedAsync(2);

        //    var emails = new List<string>();
        //    emails.AddRange(results1.Items.Select(i => i.Email));


        //    var results2 = await _tableStorage.GetAllRecordsPagedAsync(2, results1.ContinuationToken);
        //    emails.AddRange(results2.Items.Select(i => i.Email));

        //    // Assert
        //    emails.Distinct().Count().Should().Be(4);
        //}

        //[Fact]
        //public async Task get_all_records_with_entries_paged_async_returns_the_expected_count()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetAllRecordsPagedAsync();

        //    // Assert
        //    results.Items.Count.Should().Be(4);
        //}

        //[Fact]
        //public async Task get_all_records_with_entries_paged_async_returns_the_expected_count_when_given_page_size()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var results = await _tableStorage.GetAllRecordsPagedAsync(2);

        //    // Assert
        //    results.Items.Count.Should().Be(2);
        //}

        [Fact]
        public async Task get_record_async_with_an_entry_returns_the_expected_entry()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var expected = new TestTableEntity("Bill", "Jones") { Age = 45, Email = "bill.jones@somewhere.com" };

            // Act
            var result = await _tableStorage.GetRecordAsync("Jones", "Bill");

            // Assert
            result.Should().BeEquivalentTo(expected,
                op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag)
                    .Excluding(o => o.Path == "CompiledRead"));
        }

        //[Fact]
        //public async Task get_record_async_with_no_entry_returns_null()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);

        //    // Act
        //    var result = await _tableStorage.GetRecordAsync("surname", "first");

        //    // Assert
        //    result.Should().BeNull();
        //}

        [Fact]
        public async Task get_record_async_with_no_entry_returns_a_request_failed_exception_with_resource_not_found()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            Func<Task> result = async () => await _tableStorage.GetRecordAsync("surname", "first");

            // Assert
            await result.Should().ThrowAsync<RequestFailedException>().WithMessage("*ResourceNotFound*");

        }

        [Fact]
        public async Task get_record_count_async_with_entries_returns_the_expected_count()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = await _tableStorage.GetRecordCountAsync();

            // Assert
            result.Should().Be(4);
        }

        [Fact]
        public async Task get_records_by_partition_key_async_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var partitionKey = "something";

            // Act
            var result = await _tableStorage.GetByPartitionKeyAsync(partitionKey);

            // Assert
            result.Should().BeEquivalentTo(new List<TestTableEntity>());
        }

        //[Fact]
        //public async Task get_records_by_partition_key_paged_async_with_unknown_key_returns_empty_list()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);
        //    var partitionKey = "something";

        //    // Act
        //    var result = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey);

        //    // Assert
        //    result.Items.Should().BeEquivalentTo(new List<TestTableEntity>());
        //}

        [Fact]
        public async Task get_records_by_row_key_async_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var rowKey = "something";

            // Act
            var result = await _tableStorage.GetByRowKeyAsync(rowKey);

            // Assert
            result.Should().BeEquivalentTo(new List<TestTableEntity>());
        }

        //[Fact]
        //public async Task get_records_by_row_key_paged_async_with_unknown_key_returns_empty_list()
        //{
        //    // Arrange
        //    await TestDataHelper.SetupRecords(_tableStorage);
        //    var rowKey = "something";

        //    // Act
        //    var result = await _tableStorage.GetByRowKeyPagedAsync(rowKey);

        //    // Assert
        //    result.Items.Should().BeEquivalentTo(new List<TestTableEntity>());
        //}
    }
}