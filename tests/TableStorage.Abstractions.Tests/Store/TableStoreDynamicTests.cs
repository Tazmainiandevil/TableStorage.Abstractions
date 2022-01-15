using System;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Tests.Store
{
    public partial class TableStoreDynamicTests : IDisposable
    {
        private readonly ITableStoreDynamic _tableStorageDynamic;
        private const string TableName = "TestTableDynamic";
        private const string ConnectionString = "UseDevelopmentStorage=true";

        public TableStoreDynamicTests()
        {
            _tableStorageDynamic = new TableStoreDynamic(TableName, ConnectionString);
        }

        public void Dispose()
        {
            _tableStorageDynamic.DeleteTable();
        }
    }
}
