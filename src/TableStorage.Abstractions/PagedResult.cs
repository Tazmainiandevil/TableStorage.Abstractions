using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableStorage.Abstractions
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
