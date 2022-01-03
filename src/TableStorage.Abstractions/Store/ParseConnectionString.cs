using System;
using System.Collections.Generic;
using System.Linq;

namespace TableStorage.Abstractions.Store
{
    internal static class ParseConnectionString
    {
        /// <summary>
        /// The default account name for the development storage.
        /// </summary>
        internal const string UseDevelopmentStorageSetting = "UseDevelopmentStorage";
        internal const string DevstoreAccountName = "devstoreaccount1";
        internal const string DefaultEndpointsProtocolSetting = "DefaultEndpointsProtocol";
        internal const string AccountNameSetting = "AccountName";
        internal const string EndpointSuffixSetting = "EndpointSuffix";
        internal const string SharedAccessSignatureSetting = "SharedAccessSignature";
        internal const string DefaultTableHostnamePrefix = "table";

        public static Uri GetTableEndpoint(string storageConnectionString)
        {
            var settings = Parse(storageConnectionString);

            if (settings.ContainsKey(UseDevelopmentStorageSetting))
            {
                return GetDevelopmentStorageAccount();
            }

            if (!settings.ContainsKey(DefaultEndpointsProtocolSetting))
            {
                settings.Add(DefaultEndpointsProtocolSetting, "https");
            }

            var primaryUriBuilder = new UriBuilder
            {
                Scheme = settings[DefaultEndpointsProtocolSetting],
                Host = $"{settings[AccountNameSetting]}.{SettingOrDefault(EndpointSuffixSetting)}.{DefaultTableHostnamePrefix}",
                Query = SettingOrDefault(SharedAccessSignatureSetting)
            };

            string SettingOrDefault(string key)
            {
                settings.TryGetValue(key, out var result);

                return result;
            }

            return primaryUriBuilder.Uri;
        }

        private static IDictionary<string, string> Parse(string connectionString)
        {
            return connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Where(x => !string.IsNullOrEmpty(x))
                .Select(x => x.Split('='))
                .ToDictionary(s => s[0], s => s[1]);
        }

        private static Uri GetDevelopmentStorageAccount()
        {
            var builder = new UriBuilder("http", "127.0.0.1")
            {
                Path = DevstoreAccountName,
                Port = 10002
            };

            return builder.Uri;
        }
    }
}