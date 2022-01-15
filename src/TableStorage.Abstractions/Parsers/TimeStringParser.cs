using System;
using System.Runtime.CompilerServices;
using Useful.Extensions;

[assembly: InternalsVisibleTo("TableStorage.Abstractions.Tests")]

namespace TableStorage.Abstractions.Parsers
{
    internal static class TimeStringParser
    {
        private const string SecondsSuffix = "s";
        private const string MinuteSuffix = "m";
        private const string HourSuffix = "h";
        private const string DaySuffix = "d";

        public static DateTime GetTimeAgo(string ago)
        {
            return DateTime.SpecifyKind(SystemTime.UtcNow().Subtract(GetTimeAgoTimeSpan(ago)), DateTimeKind.Utc);
        }

        public static TimeSpan GetTimeAgoTimeSpan(string ago)
        {
            TimeSpan result;
            if (ago.SafeEndsWith(SecondsSuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(SecondsSuffix);
                result = TimeSpan.FromSeconds(int.Parse(timePart));
            }
            else if (ago.SafeEndsWith(MinuteSuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(MinuteSuffix);
                result = TimeSpan.FromMinutes(int.Parse(timePart));
            }
            else if (ago.SafeEndsWith(HourSuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(HourSuffix);
                result = TimeSpan.FromHours(int.Parse(timePart));
            }
            else if (ago.SafeEndsWith(DaySuffix, StringComparison.OrdinalIgnoreCase))
            {
                var timePart = ago.SubstringBeforeValue(DaySuffix);
                result = TimeSpan.FromDays(int.Parse(timePart));
            }
            else
            {
                throw new ArgumentException($"Time ago value '{ago}' is invalid. Values must be in the format of 1m, 1h, 1d.", nameof(ago));
            }

            return result;
        }
    }
}