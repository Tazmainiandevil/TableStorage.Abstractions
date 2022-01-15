using System.Collections.Generic;

namespace TableStorage.Abstractions.Models
{
    public class PagedResult<T>
    {
        public string ContinuationToken { get; }
        public IReadOnlyCollection<T> Items { get; }
        public bool IsFinalPage { get; }

        internal PagedResult(IReadOnlyCollection<T> results, string continuationToken, bool isFinalPage)
        {
            ContinuationToken = continuationToken;
            Items = results;
            IsFinalPage = isFinalPage;
        }
    }
}
