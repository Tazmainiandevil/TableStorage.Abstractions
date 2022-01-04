using Azure;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TableStorage.Abstractions.Store;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
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
            act.Should().Throw<ArgumentNullException>().WithMessage("PartitionKey cannot be null or empty*");
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
            act.Should().Throw<ArgumentNullException>().WithMessage("RowKey cannot be null or empty*");
        }

        [Fact]
        public async Task get_record_with_no_entry_returns_null()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            Action act = () => _tableStorage.GetRecord("surname", "first");

            // Assert
            act.Should().Throw<RequestFailedException>().WithMessage("The specified resource does not exist.*");
        }

        [Fact]
        public async Task get_record_with_no_entry_returns_a_request_failed_exception_with_resource_not_found()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            Action result = () => _tableStorage.GetRecord("surname", "first");

            // Assert
            result.Should().Throw<RequestFailedException>().WithMessage("*ResourceNotFound*");
        }

        [Fact]
        public async Task get_record_with_an_entry_returns_the_expected_entry()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var expected = new TestTableEntity("Bill", "Jones") { Age = 45, Email = "bill.jones@somewhere.com" };

            // Act
            var result = _tableStorage.GetRecord("Jones", "Bill");

            // Assert
            result.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path == "CompiledRead"));
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
            act.Should().Throw<ArgumentNullException>().WithMessage("PartitionKey cannot be null or empty*");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_records_by_partition_key_paged_with_null_or_empty_value_throws_exception(string partitionKey)
        {
            // Arrange
            // Act
            Action act = () => _tableStorage.GetByPartitionKeyPaged(partitionKey);

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("PartitionKey cannot be null or empty*");
        }

        [Fact]
        public async Task get_records_by_partition_key_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var partitionKey = "something";

            // Act
            var result = _tableStorage.GetByPartitionKey(partitionKey);

            // Assert
            result.Should().BeEquivalentTo(new List<TestTableEntity>());
        }

        [Fact]
        public async Task get_records_by_partition_key_paged_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var partitionKey = "something";

            // Act
            var result = _tableStorage.GetByPartitionKeyPaged(partitionKey);

            // Assert
            result.Items.Should().BeEquivalentTo(new List<TestTableEntity>());
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
        public async Task get_records_by_partition_key_with_known_key_returns_the_expected_results(string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByPartitionKey(partitionKey);

            // Assert
            results.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        [Theory]
        [MemberData(nameof(PartitionExpectedData))]
        public async Task get_records_by_partition_key_paged_with_known_key_returns_the_expected_results(string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByPartitionKeyPaged(partitionKey);

            // Assert
            results.Items.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        [Fact]
        public async Task get_records_by_partition_key_paged_after_deleted_row_has_expected_rows()
        {
            var partitionKey = "Jones";

            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            _tableStorage.Insert(new TestTableEntity("Zack", "Jones"));

            // Act
            var result = _tableStorage.GetByPartitionKeyPaged(partitionKey, pageSize: 1);
            _tableStorage.DeleteUsingWildcardEtag(new TestTableEntity("Fred", "Jones"));
            result = _tableStorage.GetByPartitionKeyPaged(partitionKey, pageSize: 1, continuationTokenJson: result.ContinuationToken);

            // Assert
            result.Items.Count.Should().Be(1);
        }

        [Theory]
        [MemberData(nameof(PartitionExpectedDataPageOfOne))]
        public async Task get_records_by_partition_key_paged_with_known_key_returns_the_expected_results_and_expected_row_count(string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByPartitionKeyPaged(partitionKey, pageSize: 1);

            // Assert
            results.Items.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        [Theory]
        [MemberData(nameof(PartitionExpectedDataPageOfOneNextPage))]
        public async Task get_records_by_partition_key_paged_with_known_key_second_page_returns_the_expected_results_and_expected_row_count(string partitionKey, List<TestTableEntity> expected)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByPartitionKeyPaged(partitionKey, pageSize: 1);
            results = _tableStorage.GetByPartitionKeyPaged(partitionKey, pageSize: 1, continuationTokenJson: results.ContinuationToken);

            // Assert
            results.Items.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        [Theory]
        [InlineData("Smith")]
        [InlineData("Jones")]
        public async Task get_records_by_partition_key_paged_with_known_key_returns_the_expected_results_with_final_page_annotated(string partitionKey)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act

            var results = _tableStorage.GetByPartitionKeyPaged(partitionKey, 1);
            results = _tableStorage.GetByPartitionKeyPaged(partitionKey, 1, results.ContinuationToken);

            // Assert
            results.IsFinalPage.Should().BeTrue();
        }

        [Fact]
        public void get_records_by_partition_key_paged_using_maximum_page_size()
        {
            var tableStore = new TableStore<TestTableEntity>("recordsbypartmaxpage", ConnectionString, _tableStorageOptions);

            for (int i = 0; i < 11; i++)
            {
                var records = new List<TestTableEntity>();
                for (int j = 0; j < 100; j++)
                {
                    records.Add(new TestTableEntity($"{i}_{j}", "x"));
                }
                tableStore.Insert(records);
            }

            var results = tableStore.GetByPartitionKeyPaged("x", 1000);
            var nextPageResults =
                tableStore.GetByPartitionKeyPaged("x", 1000, results.ContinuationToken);

            results.Items.Count.Should().Be(1000);
            nextPageResults.Items.Count.Should().Be(100);
            nextPageResults.IsFinalPage.Should().BeTrue();

            tableStore.DeleteTable();
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
            act.Should().Throw<ArgumentNullException>().WithMessage("RowKey cannot be null or empty*");
        }

        [Fact]
        public async Task get_records_by_row_key_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var rowKey = "something";

            // Act
            var result = _tableStorage.GetByRowKey(rowKey);

            // Assert
            result.Should().BeEquivalentTo(new List<TestTableEntity>());
        }

        [Fact]
        public async Task get_records_by_row_key_paged_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var rowKey = "something";

            // Act
            var result = _tableStorage.GetByRowKeyPaged(rowKey);

            // Assert
            result.Items.Should().BeEquivalentTo(new List<TestTableEntity>());
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
                        new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"},
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
                        new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                    }
                };
            }
        }

        [Theory]
        [MemberData(nameof(RowKeyExpectedData))]
        public void get_records_by_row_key_with_known_key_returns_the_expected_results(string rowKey, List<TestTableEntity> expected)
        {
            // Arrange
            TestDataHelper.SetupRowKeyRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetByRowKey(rowKey);

            // Assert
            results.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        //[Theory]
        //[MemberData(nameof(RowKeyExpectedData))]
        //public void get_records_by_row_key_with_known_key_paged_returns_the_expected_results(string rowKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    TestDataHelper.SetupRowKeyRecords(_tableStorage);

        //    // Act
        //    var results = _tableStorage.GetByRowKeyPaged(rowKey);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[MemberData(nameof(RowKeyExpectedDataPageOfOne))]
        //public void get_records_by_row_key_with_known_key_paged_returns_the_expected_results_and_expected_row_count(string rowKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    TestDataHelper.SetupRowKeyRecords(_tableStorage);

        //    // Act
        //    var results = _tableStorage.GetByRowKeyPaged(rowKey, pageSize: 1);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        //[Theory]
        //[MemberData(nameof(RowKeyExpectedDataPageOfOneNextPage))]
        //public void get_records_by_row_key_with_known_key_paged_second_page_returns_the_expected_results_and_expected_row_count(string rowKey, List<TestTableEntity> expected)
        //{
        //    // Arrange
        //    TestDataHelper.SetupRowKeyRecords(_tableStorage);

        //    // Act
        //    var results = _tableStorage.GetByRowKeyPaged(rowKey, pageSize: 1);
        //    results = _tableStorage.GetByRowKeyPaged(rowKey, pageSize: 1, continuationTokenJson: results.ContinuationToken);

        //    // Assert
        //    results.Items.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.SelectedMemberPath.EndsWith("CompiledRead")));
        //}

        [Fact]
        public void get_all_records_with_an_empty_table_returns_an_empty_list()
        {
            // Arrange
            // Act
            var results = _tableStorage.GetAllRecords();

            // Assert
            results.Should().BeEmpty();
        }

        [Fact]
        public void get_all_records_paged_with_an_empty_table_returns_an_empty_list()
        {
            // Arrange
            // Act
            var results = _tableStorage.GetAllRecordsPaged();

            // Assert
            results.Items.Should().BeEmpty();
        }

        [Fact]
        public async Task get_all_records_with_entries_returns_the_expected_count()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetAllRecords();

            // Assert
            results.Count().Should().Be(4);
        }

        [Fact]
        public void get_all_records_with_over_a_thousand_entries_returns_the_expected_count()
        {
            // Arrange

            const int recordCount = 1100;
            TestDataHelper.SetupLotsOfRecords(recordCount, _tableStorage);

            // Act
            var results = _tableStorage.GetAllRecords();

            // Assert
            results.Count().Should().Be(recordCount);
        }

        [Fact]
        public async Task get_all_records_with_entries_paged_returns_the_expected_count()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetAllRecordsPaged();

            // Assert
            results.Items.Count.Should().Be(4);
        }

        [Fact]
        public async Task get_all_records_with_entries_paged_returns_the_expected_count_when_given_page_size()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = _tableStorage.GetAllRecordsPaged(2);

            // Assert
            results.Items.Count.Should().Be(2);
        }

        [Fact]
        public async Task get_all_records_with_entries_paged_returns_the_expected_page_when_given_continuation_token()
        {
            // Arrange
            const int pageSize = 3;
            await TestDataHelper.SetupRecords(_tableStorage);
            var page1 = _tableStorage.GetAllRecordsPaged(pageSize);

            // Act
            var results = _tableStorage.GetAllRecordsPaged(pageSize, page1.ContinuationToken);

            // Assert
            results.Items.Count.Should().Be(1);
        }

        [Fact]
        public async Task get_record_count_with_entries_returns_the_expected_count()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = _tableStorage.GetRecordCount();

            // Assert
            result.Should().Be(4);
        }


        [Fact]
        public void get_record_count_with_over_a_thousand_entries_returns_the_expected_count()
        {
            // Arrange

            const int recordCount = 1100;
            TestDataHelper.SetupLotsOfRecords(recordCount, _tableStorage);

            // Act
            var result = _tableStorage.GetRecordCount();

            // Assert
            result.Should().Be(recordCount);
        }


        [Fact]
        public async Task get_records_by_filter_with_a_given_filter_returns_the_expected_count()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = _tableStorage.GetRecordsByFilter(x => x.Age >= 21 && x.Age < 29);

            // Assert
            result.Count().Should().Be(2);
        }

        [Fact]
        public async Task get_records_by_filter_with_a_given_filter_returns_the_expected_results()
        {
            // Arrange
            var expected = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
            };

            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = _tableStorage.GetRecordsByFilter(x => x.Age >= 21 && x.Age < 29);

            // Assert
            result.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

        public static IEnumerable<object[]> FilterExpectedData
        {
            get
            {
                yield return new object[]
                {
                    new List<TestTableEntity>
                    {
                        new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
                    }, 0, 1
                };
                yield return new object[]
                {
                    new List<TestTableEntity>
                    {
                        new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"}
                    }, 1, 1
                };
            }
        }

        [Theory]
        [MemberData(nameof(FilterExpectedData))]
        public async Task get_records_by_filter_with_a_given_filter_and_page_returns_the_expected_results(List<TestTableEntity> expected, int start, int page)
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var result = _tableStorage.GetRecordsByFilter(x => x.Age >= 21 && x.Age < 29, start, page);

            // Assert
            result.Should().BeEquivalentTo(expected, op => op.Excluding(o => o.Timestamp).Excluding(o => o.ETag).Excluding(o => o.Path.EndsWith("CompiledRead")));
        }

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

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("    ")]
        public void get_records_by_partition_key_paged_async_with_null_or_empty_value_throws_exception(
            string partitionKey)
        {
            // Arrange
            // Act
            Func<Task> act = async () => await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey);

            // Assert
            act.Should().ThrowAsync<ArgumentNullException>()
                .WithMessage("Value cannot be null.\r\nParameter name: partitionKey");
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

        [Fact]
        public async Task get_all_records_paged_async_with_an_empty_table_returns_an_empty_list()
        {
            // Arrange
            // Act
            var results = await _tableStorage.GetAllRecordsPagedAsync();

            // Assert
            results.Items.Should().BeEmpty();
        }

        [Fact]
        public async Task get_all_records_with_entries_paged_async_does_not_repeat_results_when_paging()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results1 = await _tableStorage.GetAllRecordsPagedAsync(2);

            var emails = new List<string>();
            emails.AddRange(results1.Items.Select(i => i.Email));

            var results2 = await _tableStorage.GetAllRecordsPagedAsync(2, results1.ContinuationToken);
            emails.AddRange(results2.Items.Select(i => i.Email));

            // Assert
            emails.Distinct().Count().Should().Be(4);
        }

        [Fact]
        public async Task get_all_records_with_entries_paged_async_returns_the_expected_count()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetAllRecordsPagedAsync();

            // Assert
            results.Items.Count.Should().Be(4);
        }

        [Fact]
        public async Task get_all_records_with_entries_paged_async_returns_the_expected_count_when_given_page_size()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);

            // Act
            var results = await _tableStorage.GetAllRecordsPagedAsync(2);

            // Assert
            results.Items.Count.Should().Be(2);
        }

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
        public async Task get_record_count_async_with_over_a_thousand_entries_returns_the_expected_count()
        {
            // Arrange

            const int recordCount = 1252;
            TestDataHelper.SetupLotsOfRecords(recordCount, _tableStorage);

            // Act
            var result = await _tableStorage.GetRecordCountAsync();

            // Assert
            result.Should().Be(recordCount);
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

        [Fact]
        public async Task get_records_by_partition_key_paged_async_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var partitionKey = "something";

            // Act
            var result = await _tableStorage.GetByPartitionKeyPagedAsync(partitionKey);

            // Assert
            result.Items.Should().BeEquivalentTo(new List<TestTableEntity>());
        }

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

        [Fact]
        public async Task get_records_by_row_key_paged_async_with_unknown_key_returns_empty_list()
        {
            // Arrange
            await TestDataHelper.SetupRecords(_tableStorage);
            var rowKey = "something";

            // Act
            var result = await _tableStorage.GetByRowKeyPagedAsync(rowKey);

            // Assert
            result.Items.Should().BeEquivalentTo(new List<TestTableEntity>());
        }
    }
}