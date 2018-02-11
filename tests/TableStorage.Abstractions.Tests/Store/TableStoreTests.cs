using System;
using FluentAssertions;
using TableStorage.Abstractions.Factory;
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

        public TableStoreTests()
        {
            _tableStorage = new TableStore<TestTableEntity>(TableName, ConnectionString);
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
            Action act = () => new TableStore<TestTableEntity>(tablename, "somestring");

            // Assert
            act.Should().Throw<ArgumentNullException>().WithMessage("Value cannot be null.\r\nParameter name: tableName");
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        public void create_table_storage_with_empty_or_null_connection_string_throws_exception(string connectionString)
        {
            // Arrange
            // Act
            Action act = () => new TableStore<TestTableEntity>("sometable", connectionString);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .WithMessage("Value cannot be null.\r\nParameter name: storageConnectionString");
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


        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        public void set_max_connections_with_null_connection_string_throws_exception(string connectionString)
        {
            // Arrange
            // Act
            Action act = () => TableStoreFactory.SetMaxNumberConnections(connectionString, 20);

            // Assert
            act.Should().Throw<ArgumentNullException>()
                .WithMessage("Value cannot be null.\r\nParameter name: storageConnectionString");
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void set_max_connections_with_less_than_1_throws_exception(int connections)
        {
            // Arrange
            // Act
            Action act = () => TableStoreFactory.SetMaxNumberConnections("test", connections);

            // Assert
            act.Should().Throw<ArgumentException>()
                .WithMessage("maxNumberOfConnections");
        }
    }
}