using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace Microsoft.CSQL.Contract
{
    public static class ServiceModifiers
    {
        public static ISymbol<TValue> CachedCall<TSource, TKey, TValue>(
           this ISymbol<TSource> source,
           Expression<Func<TSource, TKey>> cacheKeySelector,
           Expression<Func<ISymbol<TSource>, ISymbol<TValue>>> call
           )
        {
            return source
                .Call(s => new { s, r = Case_Cache<TKey, TValue>.CacheService.Get(cacheKeySelector.Compile()(s)) })
                .IfThenElse(
                    r => r.r.HasValue,
                    r => r.Call(v => v.r.Value),
                    r => r.Call(v => v.s).Call(call)
                );
        }

        public static ISymbols<TValue> CachedCall<TSource, TKey, TValue>(
           this ISymbols<TSource> source,
           Expression<Func<TSource, TKey>> cacheKeySelector,
           Expression<Func<ISymbol<TSource>, ISymbol<TValue>>> call
           )
        {
            return source
                .Call(s => new { s, r = Case_Cache<TKey, TValue>.CacheService.Get(cacheKeySelector.Compile()(s)) })
                .IfThenElse(
                    r => r.r.HasValue,
                    r => r.Call(v => v.r.Value),
                    r => r.Call(v => v.s).Call(call)
                );
        }

        public static ISymbol<T> Batch<T>(this ISymbol<T> source) // TODO: batch parameters
        {
            return source;
        }

        // partition, replication to services

        public static ISymbol<T> SLAControl<T>(this ISymbol<T> source) // TODO: SLA parameters
        {
            return source;
        }
    }
}
