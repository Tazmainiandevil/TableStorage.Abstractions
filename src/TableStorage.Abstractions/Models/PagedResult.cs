using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace TableStorage.Abstractions.Models
{
    public class PagedResult<T>
    {
        public string ContinuationToken { get; }
        public IReadOnlyCollection<T> Items { get; }
        public bool IsFinalPage { get; }

        internal PagedResult(IList<T> results, string continuationToken, bool isFinalPage)
        {
            ContinuationToken = continuationToken;
            Items = new ReadOnlyCollection<T>(results);
            IsFinalPage = isFinalPage;
        }
    }
}
