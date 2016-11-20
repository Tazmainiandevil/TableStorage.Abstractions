using System;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace TableStorage.Abstractions.Tests
{
    public partial class TableStoreAsyncTests : IDisposable
    {
        private const string TableName = "TestTableAsync";
        private const string ConnectionString = "UseDevelopmentStorage=true";
        private readonly ITableStore<TestTableEntity> tableStorage;

        public TableStoreAsyncTests()
        {
            tableStorage = new TableStore<TestTableEntity>(TableName, ConnectionString);
        }

        public void Dispose()
        {
            tableStorage.DeleteTableAsync().Wait();
        }

        [Fact]
        public async Task table_does_exist_then_exist_check_returns_true()
        {
            // Arrange
            await tableStorage.DeleteTableAsync();
            await tableStorage.CreateTableAsync();

            // Act
            var result = await tableStorage.TableExistsAsync();

            // Assert
            result.Should().BeTrue();
        }

        [Fact]
        public async Task table_does_not_exist_then_exist_check_returns_false()
        {
            // Arrange
            await tableStorage.DeleteTableAsync();

            // Act
            var result = await tableStorage.TableExistsAsync();

            // Assert
            result.Should().BeFalse();
        }
    }
}