using FluentAssertions;
using System;
using TableStorage.Abstractions.Parsers;
using Useful.Extensions;
using Xunit;

namespace TableStorage.Abstractions.Tests.Parsers
{
    public class TimeStringParserTests
    {
        public TimeStringParserTests()
        {
            SystemTime.UtcNow = () => new DateTime(2018, 01, 01, 08, 10, 00);
        }

        [Theory]
        [InlineData("10m")]
        [InlineData("10M")]
        public void given_time_string_parser_get_time_ago_when_10m_then_the_result_is_10_minutes_in_the_past(string ago)
        {
            // Arrange
            var expected = new DateTime(2018, 01, 01, 08, 00, 00);

            // Act
            var result = TimeStringParser.GetTimeAgo(ago);

            // Assert
            result.Should().Be(expected);
        }

        [Theory]
        [InlineData("1h")]
        [InlineData("1H")]
        public void given_time_string_parser_get_time_ago_when_1h_then_the_result_is_1_hour_in_the_past(string ago)
        {
            // Arrange
            var expected = new DateTime(2018, 01, 01, 07, 10, 00);

            // Act
            var result = TimeStringParser.GetTimeAgo(ago);

            // Assert
            result.Should().Be(expected);
        }

        [Theory]
        [InlineData("1d")]
        [InlineData("1D")]
        public void given_time_string_parser_get_time_ago_when_1d_then_the_result_is_1_day_in_the_past(string ago)
        {
            // Arrange
            var expected = new DateTime(2017, 12, 31, 08, 10, 00);

            // Act
            var result = TimeStringParser.GetTimeAgo(ago);

            // Assert
            result.Should().Be(expected);
        }

        [Fact]
        public void given_time_string_parser_get_time_ago_when_value_does_not_contain_valid_time_type_string_then_an_exception_is_thrown()
        {
            // Arrange
            // Act
            Action act = () => TimeStringParser.GetTimeAgo("1");

            // Assert
            act.Should().Throw<ArgumentException>().WithMessage("Time ago value '1' is invalid. Values must be in the format of 1m, 1h, 1d.\r\nParameter Name: ago");
        }

        [Fact]
        public void given_time_string_parser_get_time_ago_when_value_contains_additional_characters_after_the_valid_time_then_an_exception_is_thrown()
        {
            // Arrange
            // Act
            Action act = () => TimeStringParser.GetTimeAgo("1hdfyskdhfkds");

            // Assert
            act.Should().Throw<ArgumentException>().WithMessage("Time ago value '1hdfyskdhfkds' is invalid. Values must be in the format of 1m, 1h, 1d.\r\nParameter Name: ago");
        }
    }
}