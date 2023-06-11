using System.Collections.Generic;
using System.Threading.Tasks;
using TableStorage.Abstractions.Parsers;
using TableStorage.Abstractions.Store;
using Useful.Extensions;

namespace TableStorage.Abstractions.Tests.Helpers
{
    internal static class TestDataHelper
    {
        #region Helpers

        public static async Task SetupRecords(ITableStore<TestTableEntity> tableStorage)
        {
            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
            };

            var anotherEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
            };

            entityList.Combine(anotherEntityList);

            await tableStorage.CreateTableAsync().ConfigureAwait(false);
            await tableStorage.InsertAsync(entityList).ConfigureAwait(false);
        }

        public static async Task SetupDummyRecords(ITableStore<TestTableEntity> tableStorage, int max)
        {
            var entityList = new List<TestTableEntity>();
            for (var i = 0; i < max; i++)
            {
                entityList.Add(new TestTableEntity(i.ToString(), "x"));
            }

            await tableStorage.CreateTableAsync().ConfigureAwait(false);
            await tableStorage.InsertAsync(entityList).ConfigureAwait(false);
        }

        public static async Task SetupRecordsWithMoreThanMaxPartitionSize(ITableStore<TestTableEntity> tableStorage)
        {
            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
            };

            var anotherEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
            };

            var bigList = new List<TestTableEntity>();
            for (var i = 0; i < 105; i++)
            {
                bigList.Add(new TestTableEntity(i.ToString(), "Smith"));
            }

            entityList.Combine(anotherEntityList, bigList);

            await tableStorage.CreateTableAsync().ConfigureAwait(false);
            await tableStorage.InsertAsync(entityList).ConfigureAwait(false);
        }

        public static async Task SetupRecordsAgo(ITableStore<TestTableEntity> tableStorage, string ago)
        {
            await tableStorage.DeleteAllAsync();

            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("Kevin", "Bacon") {Age = 21, Email = "kevin.bacon@something.com"},
                new TestTableEntity("Steven", "Jones") {Age = 32, Email = "steven.jones@somewhere.com"}
            };

            await tableStorage.CreateTableAsync().ConfigureAwait(false);
            await tableStorage.InsertAsync(entityList).ConfigureAwait(false);

            await Task.Delay(TimeStringParser.GetTimeAgoTimeSpan(ago));

            var anotherEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Liam", "Matthews") {Age = 28, Email = "liam.matthews@something.com"},
                new TestTableEntity("Mary", "Gates") {Age = 45, Email = "mary.gates@somewhere.com"}
            };

            await tableStorage.InsertAsync(anotherEntityList).ConfigureAwait(false);
        }

        public static void SetupLotsOfRecords(int count, ITableStore<TestTableEntity> tableStorage)
        {
            tableStorage.CreateTable();
            for (var i = 0; i < count; i++)
            {
                var entry = new TestTableEntity($"name{i}", $"surname{count}") { Age = 32, Email = $"surname{count}@somewhere.com" };
                tableStorage.Insert(entry);
            }
        }

        public static async Task SetupRecords(ITableStoreDynamic tableStorage)
        {
            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"}
            };

            var anotherEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
            };

            entityList.Combine(anotherEntityList);

            await tableStorage.CreateTableAsync().ConfigureAwait(false);
            await tableStorage.InsertAsync(entityList).ConfigureAwait(false);
        }

        public static void SetupRowKeyRecords(ITableStore<TestTableEntity> tableStorage)
        {
            var entityList = new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"},
                new TestTableEntity("Bill", "Smith") { Age = 38, Email = "bill.smith@another.com"}
            };

            tableStorage.InsertAsync(entityList).Wait();

            var anotherEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"}
            };

            tableStorage.InsertAsync(anotherEntityList).Wait();

            var moreEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Bill", "King") {Age = 45, Email = "bill.king@email.com"}
            };

            tableStorage.InsertAsync(moreEntityList).Wait();

            var evenMoreEntityList = new List<TestTableEntity>
            {
                new TestTableEntity("Fred", "Bloggs") { Age = 32, Email = "fred.bloggs@email.com" }
            };

            tableStorage.InsertAsync(evenMoreEntityList).Wait();
        }

        public static List<TestTableEntity> GetMultiplePartitionKeyRecords()
        {
            return new List<TestTableEntity>
            {
                new TestTableEntity("John", "Smith") {Age = 21, Email = "john.smith@something.com"},
                new TestTableEntity("Jane", "Smith") {Age = 28, Email = "jane.smith@something.com"},
                new TestTableEntity("Bill", "Smith") { Age = 38, Email = "bill.smith@another.com"},
                new TestTableEntity("Fred", "Jones") {Age = 32, Email = "fred.jones@somewhere.com"},
                new TestTableEntity("Bill", "Jones") {Age = 45, Email = "bill.jones@somewhere.com"},
                new TestTableEntity("Bill", "King") { Age = 45, Email = "bill.king@email.com"},
                new TestTableEntity("Fred", "Bloggs") { Age = 32, Email = "fred.bloggs@email.com" }
            };
        }

        public static List<TestTableEntity> GetMoreThanMaxSinglePartitionRecords()
        {
            var entryList = new List<TestTableEntity>();
            for (var i = 0; i < 105; i++)
            {
                entryList.Add(new TestTableEntity(i.ToString(), "x"));
            }

            return entryList;
        }

        public static List<TestTableEntity> GetMoreThanMaxMultiplePartitionRecords()
        {
            var entryList = GetMoreThanMaxSinglePartitionRecords();

            for (var i = 0; i < 105; i++)
            {
                entryList.Add(new TestTableEntity($"a{i}", "y"));
            }

            return entryList;
        }

        #endregion Helpers
    }
}