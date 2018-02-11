using FluentAssertions;
using FluentValidation;
using System;
using TableStorage.Abstractions.Models;
using TableStorage.Abstractions.Store;
using TableStorage.Abstractions.Tests.Helpers;
using Xunit;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreTests : IDisposable
    {
        private const string TableName = "TestTable";
        private const string ConnectionString = "UseDevelopmentStorage=true";
        private readonly ITableStore<TestTableEntity> _tableStorage;
        private readonly TableStorageOptions _tableStorageOptions = new TableStorageOptions();

        public TableStoreTests()
        {
            _tableStorage = new TableStore<TestTableEntity>(TableName, ConnectionString, _tableStorageOptions);
        }

        public void Dispose()
        {
            _tableStorage.DeleteTable();
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        public void create_table_storage_with_empty_or_null_table_name_throws_exception(string tablename)
        {
            // Arrange
            // Act
            Action act = () => new TableStore<TestTableEntity>(tablename, "somestring", _tableStorageOptions);

            // Assert
            act.Should().Throw<ArgumentException>().WithMessage("Table name cannot be null or empty\r\nParameter name: tableName");
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        public void create_table_storage_with_empty_or_null_connection_string_throws_exception(string connectionString)
        {
            // Arrange
            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", connectionString, _tableStorageOptions);

            // Assert
            act.Should().Throw<ArgumentException>()
                .WithMessage("Table connection string cannot be null or empty\r\nParameter name: storageConnectionString");
        }

        [Fact]
        public void create_table_storage_with_null_table_options_throws_exception()
        {
            // Arrange
            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", ConnectionString, null);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .WithMessage("Table storage options cannot be null\r\nParameter name: options");
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(1)]
        public void create_table_storage_with_table_option_connection_limit_less_than_2_then_throws_an_exception(int connectionLimit)
        {
            // Arrange
            var options = new TableStorageOptions { ConnectionLimit = connectionLimit };

            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", ConnectionString, options);

            // Assert
            act.Should().Throw<ValidationException>()
                .WithMessage("Validation failed: \r\n -- 'Connection Limit' must be greater than or equal to '2'.");
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        public void create_table_storage_with_table_options_retries_less_than_1_then_throws_an_exception(int retries)
        {
            // Arrange
            var options = new TableStorageOptions { Retries = retries };

            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", ConnectionString, options);

            // Assert
            act.Should().Throw<ValidationException>()
                .WithMessage("Validation failed: \r\n -- 'Retries' must be greater than '0'.");
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(0)]
        public void create_table_storage_with_table_options_retry_wait_in_seconds_less_than_1_then_throws_an_exception(double retryTime)
        {
            // Arrange
            var options = new TableStorageOptions { RetryWaitTimeInSeconds = retryTime };

            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", ConnectionString, options);

            // Assert
            act.Should().Throw<ValidationException>()
                .WithMessage("Validation failed: \r\n -- 'Retry Wait Time In Seconds' must be greater than '0'.");
        }

        [Theory]
        [InlineData(-1, -1, -1)]
        [InlineData(0, 0, 0)]
        [InlineData(1, 0, 0)]
        public void create_table_storage_with_multiple_invalid_table_options_throws_an_exception_with_all_invalid_entries(int connectionLimit, int retries, double retryTime)
        {
            // Arrange
            var options = new TableStorageOptions { ConnectionLimit = connectionLimit, Retries = retries, RetryWaitTimeInSeconds = retryTime };

            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", ConnectionString, options);

            // Assert
            act.Should().Throw<ValidationException>()
                .WithMessage("Validation failed: \r\n -- 'Connection Limit' must be greater than or equal to '2'.\r\n -- 'Retries' must be greater than '0'.\r\n -- 'Retry Wait Time In Seconds' must be greater than '0'.");
        }

        [Fact]
        public void table_does_exist_then_exist_check_returns_true()
        {
            // Arrange
            _tableStorage.DeleteTable();

            // Act
            _tableStorage.CreateTable();

            // Assert
            _tableStorage.TableExists().Should().BeTrue();
        }

        [Fact]
        public void table_does_not_exist_then_exist_check_returns_false()
        {
            // Arrange
            _tableStorage.DeleteTable();

            // Act
            var result = _tableStorage.TableExists();

            // Assert
            result.Should().BeFalse();
        }
    }
}