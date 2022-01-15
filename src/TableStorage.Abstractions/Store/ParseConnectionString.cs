using System;

namespace TableStorage.Abstractions.Store
{
    internal static class ParseConnectionString
    {
        /// <summary>
        /// As CloudStorageAccount.Parse is not available in Azure.Data.Tables and the equivalent StorageConnectionString
        /// is an internal class this functionality can only be obtained by custom code or reflection
        /// Hopefully the need to do this in the future will be a public parse method in Azure Storage Common library
        /// </summary>
        /// <param name="storageConnectionString">The connection string</param>
        /// <returns></returns>
        public static Uri GetTableEndpoint(string storageConnectionString)
        {
            var storageConnectionStringType = Type.GetType("Azure.Storage.StorageConnectionString, Azure.Storage.Common");

            var storageConnectionStringObject = storageConnectionStringType?.GetMethod("Parse", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
                ?.Invoke(null, new object[] { storageConnectionString });

            var tableEndpoint = storageConnectionStringType?.GetProperty("TableEndpoint", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                ?.GetValue(storageConnectionStringObject);

            return tableEndpoint as Uri;
        }
    }
}