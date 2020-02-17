using FluentValidation;
using TableStorage.Abstractions.Store;

namespace TableStorage.Abstractions.Validators
{
    /// <summary>
    /// Validate the Table Storage Options
    /// </summary>
    public class TableStorageOptionsValidator : AbstractValidator<TableStorageOptions>
    {
        public TableStorageOptionsValidator()
        {
            RuleFor(x => x.ConnectionLimit).GreaterThanOrEqualTo(2);
            RuleFor(x => x.Retries).GreaterThan(0);
            RuleFor(x => x.RetryWaitTimeInSeconds).GreaterThan(0);
        }
    }
}