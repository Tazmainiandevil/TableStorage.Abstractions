using System;
using System.Runtime.CompilerServices;
using Useful.Extensions;

[assembly: InternalsVisibleTo("TableStorage.Abstractions.Tests")]

namespace TableStorage.Abstractions.Parsers
{
    internal static class TimeStringParser
    {
        private const string MinuteSuffix = "m";
        private const string HourSuffix = "h";
        private const string DaySuffix = "d";

        public static DateTime GetTimeAgo(string ago)
        {
            DateTime result;            
            if (ago.SafeEndsWith(MinuteSuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(MinuteSuffix);
                var fromMinutes = TimeSpan.FromMinutes(int.Parse(timePart));

                result = SystemTime.UtcNow().Subtract(fromMinutes);
            }
            else if (ago.SafeEndsWith(HourSuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(HourSuffix);
                var fromHours = TimeSpan.FromHours(int.Parse(timePart));

                result = SystemTime.UtcNow().Subtract(fromHours);
            }
            else if (ago.SafeEndsWith(DaySuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(DaySuffix);
                var fromDays = TimeSpan.FromDays(int.Parse(timePart));

                result = SystemTime.UtcNow().Subtract(fromDays);
            }
            else
            {
                throw new ArgumentException($"Time ago value '{ago}' is invalid. Values must be in the format of 1m, 1h, 1d.", nameof(ago));
            }

            return result;
        }
    }
}