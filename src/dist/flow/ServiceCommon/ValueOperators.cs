/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */
 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace rDSN.Tron.Runtime
{
    public static class ConcreteOperators
    {
        #region call (either through service or stateless local call or further queries)

        private class MyPoolWork<T>
        {
            public MyPoolWork(Action<T> call)
            {
                _call = call;
            }

            public void Execute(object s)
            {
                _call((T)s);
            }

            private Action<T> _call;
        }

        public static IEnumerable<TSource> AsyncCall<TSource>(
            this IEnumerable<TSource> source,
            Action<TSource> call
            )
        {
            foreach (var s in source)
            {
                ThreadPool.QueueUserWorkItem((new MyPoolWork<TSource>(call)).Execute, s);
                yield return s;
            }
        }

        public static IValue<TSource> AsyncCall<TSource>(
            this IValue<TSource> source,
            Action<TSource> call
            )
        {
            ThreadPool.QueueUserWorkItem((new MyPoolWork<TSource>(call)).Execute, source.Value());
            return source;
        }

        public static IEnumerable<TSource> AsyncCallEx<TSource>(
            this IEnumerable<TSource> source,
            Action<TSource> call
            )
        {
            foreach (var s in source)
            {
                ThreadPool.QueueUserWorkItem((new MyPoolWork<TSource>(call)).Execute, s);
                yield return s;
            }
        }

        public static IValue<TSource> AsyncCallEx<TSource>(
            this IValue<TSource> source,
            Action<TSource> call
            )
        {
            ThreadPool.QueueUserWorkItem((new MyPoolWork<TSource>(call)).Execute, source.Value());
            return source;
        }

        public static IEnumerable<TResult> CallEx<TSource, TResult>(
            this IEnumerable<TSource> source,
            Func<IValue<TSource>, IValue<TResult>> call
            )
        {
            return source.Select(r => call(new IValue<TSource>(r)).Value());
        }

        public static IEnumerable<TResult> Call<TSource, TResult>(
            this IEnumerable<TSource> source,
            Func<TSource, TResult> call
            )
        {
            return source.Select(call);
        }

        public static IValue<TResult> Call<TSource, TResult>(
            this IValue<TSource> source,
            Func<TSource, TResult> call
            )
        {
            return new IValue<TResult>(call(source.Value()));
        }

        public static IValue<TResult> CallEx<TSource, TResult>(
            this IValue<TSource> source,
            Func<IValue<TSource>, IValue<TResult>> call
            )
        {
            return call(source);
        }

        #endregion call

        #region scatter and gather

        /// <summary>
        /// extract a collection of elements from one input object
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TResult"> output element type </typeparam>
        /// <param name="source"> input object </param>
        /// <param name="extractor"> extracting logic </param>
        /// <returns> a collection of output elements </returns>

        public static IEnumerable<TResult> Scatter<TSource, TResult>(
            this IValue<TSource> source,
            Func<TSource, IEnumerable<TResult>> extractor
            )
        {
            return extractor(source.Value());
        }

        /// <summary>
        /// extract a collection of elements from each object in a input collection, and merge into a new collection
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TResult"> output element type </typeparam>
        /// <param name="source"> input collection </param>
        /// <param name="extractor"> extracting logic </param>
        /// <returns> a collection of output elements </returns>

        public static IEnumerable<TResult> Scatter<TSource, TResult>(
            this IEnumerable<TSource> source,
            Func<TSource, IEnumerable<TResult>> extractor
            )
        {
            return source.SelectMany(extractor);
        }


        public static IEnumerable<TSource> Scatter<TSource>(
            this IValue<IEnumerable<TSource>> source
            )
        {
            return source.Value();
        }


        public static IEnumerable<TSource> Scatter<TSource>(
            this IEnumerable<IEnumerable<TSource>> source
            )
        {
            return source.SelectMany(s => s);
        }

        //
        //public static IValue<TResult> Gather<TSource, TResult>(
        //    this IEnumerable<TSource> source,
        //    Func<IEnumerable<TSource>, TResult> reducer
        //    )
        //{
        //    return source.CreateQuery<TResult>(
        //        Expression.Call(null,
        //                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
        //                new Expression[] { source.Expression, reducer }
        //                )
        //        );
        //}

        /// <summary>
        /// convert a collection of input object into a single value
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TResult"> output object type </typeparam>
        /// <param name="source"> input object collection </param>
        /// <param name="reducer"> aggregation function </param>
        /// <returns> single value result </returns>

        public static IValue<TResult> Gather<TSource, TResult>(
            this IEnumerable<TSource> source,
            Func<IEnumerable<TSource>, TResult> reducer
            )
        {
            return new IValue<TResult>(reducer(source));
        }

        //
        //public static IEnumerable<TResult> Gather<TSource, TResult>(
        //    this IEnumerable<TSource> source,
        //    Func<IEnumerable<TSource>, IEnumerable<TResult>>> reducer
        //    )
        //{
        //    return source.CreateCollectionQuery<TResult>(
        //        Expression.Call(null,
        //                ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
        //                new Expression[] { source.Expression, reducer }
        //                )
        //        );
        //}

        /// <summary>
        /// group a collection of input objects and convert each group into a single value
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TResult"> output object type </typeparam>
        /// <param name="source"> input object collection </param>
        /// <param name="keySelector"> group key selector </param>
        /// <param name="reducer"> aggregation function </param>
        /// <returns> aggregation result for each group </returns>

        public static IEnumerable<TResult> Gather<TSource, TReduceKey, TResult>(
            this IEnumerable<TSource> source,
            Func<TSource, TReduceKey> keySelector,
            Func<IEnumerable<TSource>, TResult> reducer
            )
        {
            return source.GroupBy(keySelector).Select(reducer);
        }

        /// <summary>
        /// Applies an increamental accumulator function over a sequence. The specified seed value
        //  is used as the initial accumulator value.
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TAccumulate"> output object type </typeparam>
        /// <param name="source"> input object sequence </param>
        /// <param name="seed"> initial value of aggregation </param>
        /// <param name="func"> aggregation function </param>
        /// <returns></returns>

        public static IValue<TAccumulate> Gather<TSource, TAccumulate>(
            this IEnumerable<TSource> source,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> func
            )
        {
            return new IValue<TAccumulate>(source.Aggregate(seed, func));
        }

        /// <summary>
        /// Applies an increamental accumulator function over each group of an input sequence grouped by keySelector. 
        /// The specified seed value is used as the initial accumulator value for each group.
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TAccumulate"> output object type </typeparam>
        /// <param name="source"> input object sequence </param>
        /// <param name="keySelector"> group key selector </param>
        /// <param name="seed"> initial value of aggregation </param>
        /// <param name="func"> aggregation function </param>
        /// <returns></returns>

        public static IEnumerable<TAccumulate> Gather<TSource, TReduceKey, TAccumulate>(
            this IEnumerable<TSource> source,
            Func<TSource, TReduceKey> keySelector,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> reducer
            )
        {
            return source.GroupBy(keySelector).Select(g => g.Aggregate(seed, reducer));
        }

        #endregion scatter and gather

        #region control flow (branch, loop)

        public class ValIfPair<T>
        {
            public IValue<T> Then;
            public IValue<T> Else;
        };

        public class ValIfPairs<T>
        {
            public IEnumerable<T> Then;
            public IEnumerable<T> Else;
        };

        public static ValIfPair<TSource> If<TSource>(
            this IValue<TSource> source,
            Func<TSource, bool> ifPredicate
            )
        {
            var r = new ValIfPair<TSource>();

            if (ifPredicate(source.Value()))
                r.Then = source;
            else
                r.Else = source;

            return r;
        }

        public static ValIfPairs<TSource> If<TSource>(
            this IEnumerable<TSource> source,
            Func<TSource, bool> ifPredicate
            )
        {
            var r = new ValIfPairs<TSource>
            {
                Then = source.Where(ifPredicate),
                Else = source.Where(x => ifPredicate(x) == false)
            };
            return r;
        }

        public static IValue<TResult> IfThenElse<TSource, TResult>(
            this IValue<TSource> source,
            Func<TSource, bool> ifPredicate,
            Func<IValue<TSource>, IValue<TResult>> thenBlock,
            Func<IValue<TSource>, IValue<TResult>> elseBlock
            )
        {
            if (ifPredicate(source.Value()))
                return thenBlock(source);
            else
                return elseBlock(source);
        }

        public static IEnumerable<TResult> IfThenElse<TSource, TResult>(
            this IEnumerable<TSource> source,
            Func<TSource, bool> ifPredicate,
            Func<IValue<TSource>, IValue<TResult>> thenBlock,
            Func<IValue<TSource>, IValue<TResult>> elseBlock
            )
        {
            foreach (var v in source)
            {
                if (ifPredicate(v))
                    yield return thenBlock(new IValue<TSource>(v)).Value();
                else
                    yield return elseBlock(new IValue<TSource>(v)).Value();
            }
        }

        #endregion  control flow (branch, loop)

        // modifiers
        public static IValue<TSource> Assign<TSource>(
            this IValue<TSource> source,
            out IValue<TSource> var
            )
        {
            var = source;
            return source;
        }

        public static IEnumerable<TSource> Assign<TSource>(
            this IEnumerable<TSource> source,
            out IEnumerable<TSource> var
            )
        {
            var = source;
            return source;
        }
    }
}
