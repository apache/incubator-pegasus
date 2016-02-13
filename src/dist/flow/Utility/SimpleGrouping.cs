using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public class SimpleGrouping<TKey, TElement> : List<TElement>, IGrouping<TKey, TElement>
    {
        public TKey Key { get; set; }
    }
}
