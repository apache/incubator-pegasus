using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public static class LinqHelper
    {
        public static string VerboseCombine<T>(this IEnumerable<T> source, string seperator, Func<T, string> selector)
        {
            string s = "";
            source.Select(v => { s += selector(v) + seperator; return 0; }).Count();
            if (s.Length > 0)
            {
                s = s.Substring(0, s.Length - seperator.Length);
            }
            return s;
        }

        public static IEnumerable<TSource> DistinctBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            HashSet<TKey> keys = new HashSet<TKey>();
            foreach (TSource e in source)
            {
                if (keys.Add(keySelector(e)))
                {
                    yield return e;
                }
            }
        }

        public static IEnumerable<TSource> Top<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector, int count, bool isAscending)
        {
            if (isAscending)
                return source.OrderBy(keySelector).Take(count);
            else
                return source.OrderByDescending(keySelector).Take(count);
        }
    }
}
