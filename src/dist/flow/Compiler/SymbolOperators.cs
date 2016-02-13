////////////////////////////////////////////////////////////////////////////////
// Copyright (c) Microsoft Corporation.  All Rights Reserved.
//
// File Name: SymbolOperator.cs
//
// Description:
//
//   Language Primitives for service composition
//
// Notes:
//
// Change history:
//   @06/22/2014 - zhenyug
//
// Owners:
//
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Diagnostics;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;
using rDSN.Tron.Runtime;

namespace rDSN.Tron.Compiler
{
    public static class Csql
    {
        #region call (either through service or stateless local call or further queries)

        /// <summary>
        /// transform an input source to an output result
        /// </summary>
        /// <typeparam name="TSource"> input type </typeparam>
        /// <typeparam name="TResult"> output type </typeparam>
        /// <param name="source"> input variable </param>
        /// <param name="call"> transformation process, could be either UDF or service call, or other LINQ expressions </param>
        /// <returns> transformed result </returns>
        [Primitive]
        public static ISymbol<TResult> Call<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, TResult>> call
            )
        {
            return source.CreateQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }


        /// <summary>
        /// Similar to Call, except that the transformation is applied to each element in source
        /// </summary>
        [Primitive]
        public static ISymbolCollection<TResult> Call<TSource, TResult>(
            this ISymbolCollection<TSource> source,
            Expression<Func<TSource, TResult>> call
            )
        {
            return source.CreateCollectionQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        /// <summary>
        /// apply transformation to an input source in an asynchronous fashion
        /// </summary>
        /// <typeparam name="TSource"> input type </typeparam>
        /// <param name="source"> intput variable </param>
        /// <param name="call"> transformation process, could be either UDF or service call, or other LINQ expressions </param>
        /// <returns> the input variable </returns>
        [Primitive]
        public static ISymbol<TSource> AsyncCall<TSource>(
            this ISymbol<TSource> source,
            Expression<Action<TSource>> call
            )
        {
            return source.CreateQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        /// <summary>
        /// Similar to AsyncCall, except that the transformation is applied to each element in source
        /// </summary>
        [Primitive]
        public static ISymbolCollection<TSource> AsyncCall<TSource>(
            this ISymbolCollection<TSource> source,
            Expression<Action<TSource>> call
            )
        {
            return source.CreateCollectionQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource)}),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ///
        /// following are enhanced version with new semantic scopes
        ///
        
        /// <summary>
        /// Similar to Call, except that the transformation process can be another TRON statement
        /// </summary>
        [Primitive]
        public static ISymbol<TResult> CallEx<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> call
            )
        {
            return source.CreateQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        /// <summary>
        /// Similar to Call, except that the transformation process can be another TRON statement
        /// </summary>
        [Primitive]
        public static ISymbolCollection<TResult> CallEx<TSource, TResult>(
            this ISymbolCollection<TSource> source,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> call
            )
        {
            return source.CreateCollectionQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        /// <summary>
        /// Similar to AsyncCall, except that the transformation process can be another TRON statement
        /// </summary>
        [Primitive]
        public static ISymbol<TSource> AsyncCallEx<TSource>(
            this ISymbol<TSource> source,
            Expression<Action<ISymbol<TSource>>> call
            )
        {
            return source.CreateQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression, call }
                        )
                );
        }

        /// <summary>
        /// Similar to AsyncCall, except that the transformation process can be another TRON statement
        /// </summary>
        [Primitive]
        public static ISymbolCollection<TSource> AsyncCallEx<TSource>(
            this ISymbolCollection<TSource> source,
            Expression<Action<ISymbol<TSource>>> call
            )
        {
            return source.CreateCollectionQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression, call }
                        )
                );
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
        [Primitive]
        public static ISymbolCollection<TResult> Scatter<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, IEnumerable<TResult>>> extractor
            )
        {
            return source.CreateCollectionQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, extractor }
                        )
                );
        }

        /// <summary>
        /// extract a collection of elements from each object in a input collection, and merge into a new collection
        /// </summary>
        /// <typeparam name="TSource"> input object type </typeparam>
        /// <typeparam name="TResult"> output element type </typeparam>
        /// <param name="source"> input collection </param>
        /// <param name="extractor"> extracting logic </param>
        /// <returns> a collection of output elements </returns>
        [Primitive]
        public static ISymbolCollection<TResult> Scatter<TSource, TResult>(
            this ISymbolCollection<TSource> source,
            Expression<Func<TSource, IEnumerable<TResult>>> extractor
            )
        {
            return source.CreateCollectionQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, extractor }
                        )
                );
        }

        [Primitive]
        public static ISymbolCollection<TSource> Scatter<TSource>(
            this ISymbol<IEnumerable<TSource>> source
            )
        {
            return source.CreateCollectionQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression }
                        )
                );
        }

        [Primitive]
        public static ISymbolCollection<TSource> Scatter<TSource>(
            this ISymbolCollection<IEnumerable<TSource>> source
            )
        {
            return source.CreateCollectionQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource)}),
                        new Expression[] { source.Expression }
                        )
                );
        }

        //[Primitive]
        //public static ISymbol<TResult> Gather<TSource, TResult>(
        //    this ISymbolCollection<TSource> source,
        //    Expression<Func<IEnumerable<TSource>, TResult>> reducer
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
        [Primitive]
        public static ISymbol<TResult> Gather<TSource, TResult>(
            this ISymbolCollection<TSource> source,
            Expression<Func<IEnumerable<TSource>, TResult>> reducer
            )
        {
            return source.CreateQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, reducer }
                        )
                );
        }

        //[Primitive]
        //public static ISymbolCollection<TResult> Gather<TSource, TResult>(
        //    this ISymbolCollection<TSource> source,
        //    Expression<Func<IEnumerable<TSource>, IEnumerable<TResult>>> reducer
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
        [Primitive]
        public static ISymbolCollection<TResult> Gather<TSource, TReduceKey, TResult>(
            this ISymbolCollection<TSource> source,
            Expression<Func<TSource, TReduceKey>> keySelector,
            Expression<Func<IEnumerable<TSource>, TResult>> reducer
            )
        {
            return source.CreateCollectionQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, keySelector, reducer }
                        )
                );
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
        [Primitive]
        public static ISymbol<TAccumulate> Gather<TSource, TAccumulate>(
            this ISymbolCollection<TSource> source, 
            TAccumulate seed, 
            Expression<Func<TAccumulate, TSource, TAccumulate>> func
            )
        {
            return source.CreateQuery<TAccumulate>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TAccumulate) }),
                        new Expression[] { source.Expression, Expression.Constant(seed), func }
                        )
                );
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
        [Primitive]
        public static ISymbolCollection<TAccumulate> Gather<TSource, TReduceKey, TAccumulate>(
            this ISymbolCollection<TSource> source,
            Expression<Func<TSource, TReduceKey>> keySelector,
            TAccumulate seed,
            Expression<Func<TAccumulate, TSource, TAccumulate>> reducer
            )
        {
            return source.CreateCollectionQuery<TAccumulate>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TReduceKey), typeof(TAccumulate) }),
                        new Expression[] { source.Expression, keySelector, Expression.Constant(seed), reducer }
                        )
                );
        }

        #endregion scatter and gather
        
        #region control flow (branch, loop)

        [Primitive]
        public static ISymbol<TResult> IfThenElse<TSource, TResult>(
            this ISymbol<TSource> source,
            Expression<Func<TSource, bool>> ifPredicate,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> thenBlock,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> elseBlock
            )
        {
            return source.CreateQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, ifPredicate, thenBlock, elseBlock }
                        )
                );
        }

        [Primitive]
        public static ISymbolCollection<TResult> IfThenElse<TSource, TResult>(
            this ISymbolCollection<TSource> source,
            Expression<Func<TSource, bool>> ifPredicate,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> thenBlock,
            Expression<Func<ISymbol<TSource>, ISymbol<TResult>>> elseBlock
            )
        {
            return source.CreateCollectionQuery<TResult>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource), typeof(TResult) }),
                        new Expression[] { source.Expression, ifPredicate, thenBlock, elseBlock }
                        )
                );
        }


        /*
         e.g.,
         *  do
         *  {
         *      loopBody(context);
         *  }
         *  while (!exitPredicate(context));
         */
        [Primitive]
        public static ISymbol<TSource> DoWhile<TSource>(
            this ISymbol<TSource> source,
            Expression<Func<ISymbol<TSource>, ISymbol<TSource>>> body,
            Expression<Func<TSource, bool>> condition
            )
        {
            return source.CreateQuery<TSource>(
                    Expression.Call(null,
                            ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource)}),
                            new Expression[] { source.Expression, body, condition }
                            )
                    );
        }

        [Primitive]
        public static ISymbolCollection<TSource> DoWhile<TSource>(
            this ISymbolCollection<TSource> source,
            Expression<Func<ISymbol<TSource>, ISymbol<TSource>>> body,
            Expression<Func<TSource, bool>> condition
            )
        {
            return source.CreateCollectionQuery<TSource>(
                    Expression.Call(null,
                            ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                            new Expression[] { source.Expression, body, condition }
                            )
                    );
        }

        #endregion

        #region compiler utilities

        [Primitive(AnalyzerType = typeof(AliasHelper.AssignPrimitiveAnalyzer))]
        public static ISymbol<TSource> Assign<TSource>(
            this ISymbol<TSource> source,
            out ISymbol<TSource> var
            )
        {
            var = source;
            return source;
        }

        public static IEnumerable<T> Many<T>(this T result)
        {
            throw new NotImplementedException("impossible execution path");
        }

        //public static string ToAssembly<T>(this ISymbol<T> result)
        //{
        //    Trace.Assert(result.Expression is MethodCallExpression);
        //    return Compiler.Compile(result.Expression as MethodCallExpression, result.GetType().IsSymbols());
        //}

        //public static ServiceMesh<TRequest, TResponse> CreateService<TRequest, TResponse>(string name, Func<ISymbol<TRequest>, ISymbol<TResponse>> call, SLA sla)
        //{
        //    ISymbol<TRequest> query = new ISymbol<TRequest>("request");
        //    var result = call(query);
        //    var mesh = ServiceMesh.Load(result.ToAssembly(), name, sla) as ServiceMesh<TRequest, TResponse>;
        //    Trace.Assert(mesh.InputType == typeof(TRequest));
        //    return mesh;
        //}

        public static ServicePlan CreateService<TServiceImpl>(string name)
        {
            var plan = Compiler.Compile(typeof(TServiceImpl));

            Console.WriteLine("Composed service '" + typeof(TServiceImpl).Name + "' built in '" + plan.Package.Name + "'\r\n"
                + "To publish the service package:\r\n"
                + "\ttron pp " + plan.Package.Name + "\r\n"
                + "To deploy an instance of this service with name %name%:\r\n"
                + "\ttron cs " + typeof(TServiceImpl).Name + " %name%\r\n"
                + "To get client lib for this service and built further client apps:\r\n"
                + "\ttron gsc " + typeof(TServiceImpl).Name
                );

            return plan;
        }

        #endregion 

    }
}
