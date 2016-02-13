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
    public static class SymbolicOperators
    {
        #region call (either through service or stateless local call or further queries)

        public static ISymbols<TSource> AsyncCall<TSource>(
            this ISymbols<TSource> source,
            Expression<Action<TSource>> call
            )
        {
            return source.Provider.CreateSymbolsQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource)}),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        public static ISymbol<TSource> AsyncCall<TSource>(
            this ISymbol<TSource> source,
            Expression<Action<TSource>> call
            )
        {
            return source.Provider.CreateSymbolQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }


        public static ISymbol<TResult> Call<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, TResult>> call
            )
        {
            return source.Provider.CreateSymbolQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }


        public static ISymbol<TResult> Call<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> call
            )
        {
            return source.Provider.CreateSymbolQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }
        
        public static ISymbols<TResult> Call<TSource, TResult>(
            this ISymbols<TSource> source,
            Expression<Func<TSource, TResult>> call
            )
        {
            return source.Provider.CreateSymbolsQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        public static ISymbols<TResult> Call<TSource, TResult>(
            this ISymbols<TSource> source,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> call
            )
        {
            return source.Provider.CreateSymbolsQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        #endregion call

        #region ordering and top

        public static ISymbols<TSource> Top<TSource, TKey>(
            this ISymbols<TSource> source,
            Expression<Func<TSource, TKey>> keySelector,
            int count,
            bool isAscending
            )
        {
            return source.Provider.CreateSymbolsQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression, Expression.Constant(count) }
                        )
                );
        }

        #endregion

        #region scatter and gather

        public static ISymbols<TResult> Scatter<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, IEnumerable<TResult>>> extractor
            )
        {
            return source.Provider.CreateSymbolsQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, extractor }
                        )
                );
        }

        public static ISymbol<TResult> Gather<TSource, TResult>(
            this ISymbols<TSource> source,
            Expression<Func<IEnumerable<TSource>, TResult>> reducer
            )
        {
            return source.Provider.CreateSymbolQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, reducer }
                        )
                );
        }

        public static ISymbols<TResult> Gather<TSource, TReduceKey, TResult>(
            this ISymbols<TSource> source,
            Expression<Func<TSource, TReduceKey>> keySelector,
            Expression<Func<IEnumerable<TSource>, TResult>> reducer
            )
        {
            return source.Provider.CreateSymbolsQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, keySelector, reducer }
                        )
                );
        }

        public static TResult Gather<TSource, TResult>(
            this IEnumerable<TSource> query,
            Func<IEnumerable<TSource>, TResult> reducer
            )
        {
            return reducer(query);
        }

        #endregion scatter and gather
        
        #region control flow (branch, loop)

        public static ISymbol<TResult> IfThenElse<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, bool>> ifPredicate,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> thenBlock,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> elseBlock
            )
        {
            return source.Provider.CreateSymbolQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, ifPredicate, thenBlock, elseBlock }
                        )
                );
        }

        public static ISymbols<TResult> IfThenElse<TSource, TResult>(
            this ISymbols<TSource> source,
            Expression<Func<TSource, bool>> ifPredicate,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> thenBlock,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> elseBlock
            )
        {
            return source.Provider.CreateSymbolsQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, ifPredicate, thenBlock, elseBlock }
                        )
                );
        }


        /*
         e.g.,
         *  var context = contextInit(source);
         *  while (!exitPredicate(context))
         *  {
         *      loopBody(context);
         *  }
         *  result = resultExtractor(context);
         */
        public static ISymbol<TResult> WhileDo<TSource, TContext, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, TContext>> contextInit,
            Expression<Func<TContext, bool>> exitPredicate,
            Expression<Action<ISymbol<TContext>>> loopBody,
            Expression<Func<TContext, TResult>> resultExtractor
            )
        {
            return source.Provider.CreateSymbolQuery<TResult>(
                    Expression.Call(null,
                            ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TContext), typeof(TResult) }),
                            new Expression[] { source.Expression, contextInit, exitPredicate, loopBody, resultExtractor }
                            )
                    );
        }

        public static ISymbols<TResult> WhileDo<TSource, TContext, TResult>(
            this ISymbols<TSource> source,
            Expression<Func<TSource, TContext>> contextInit,
            Expression<Func<TContext, bool>> exitPredicate,
            Expression<Action<ISymbol<TContext>>> loopBody,
            Expression<Func<TContext, TResult>> resultExtractor
            )
        {
            return source.Provider.CreateSymbolsQuery<TResult>(
                    Expression.Call(null,
                            ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TContext), typeof(TResult) }),
                            new Expression[] { source.Expression, contextInit, exitPredicate, loopBody, resultExtractor }
                            )
                    );
        }

        #endregion

        // modifiers

    }
}
