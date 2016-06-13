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
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.CSQL.Contract
{
    public static class AliasHelper
    {
        static ThreadLocal<Dictionary<string, Queue<object>>> tNamedObjects
            = new ThreadLocal<Dictionary<string, Queue<object>>>(() => new Dictionary<string, Queue<object>>());

        // define a var, local or global
        public static IValue<TSource> Alias<TSource>(
            this IValue<TSource> source,
            string variableName
            )
        {
            Queue<object> q;
            if (!tNamedObjects.Value.TryGetValue(variableName, out q))
            {
                q = new Queue<object>();
                tNamedObjects.Value.Add(variableName, q);
            }
            q.Enqueue(source);

            return source;
        }

        public static IValue<T> GetValue<T>(string variableName)
        {
            Queue<object> q;
            if (tNamedObjects.Value.TryGetValue(variableName, out q))
            {
                object v = q.Dequeue();
                if (q.Count == 0)
                    tNamedObjects.Value.Remove(variableName);
                return v as IValue<T>;
            }
            else
            {
                throw new NotImplementedException("cannot find specified variable named '" + variableName + "'");
            }
        }

        // define a var, local or global
        public static ISymbol<TSource> Alias<TSource>(
            this ISymbol<TSource> source,
            string variableName
            )
        {
            return source.Provider.CreateSymbolQuery<TSource>(
                Expression.Call(null,
                        ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(new Type[] { typeof(TSource) }),
                        new Expression[] { source.Expression, Expression.Constant(variableName) }
                        )
                );
        }

        public static ISymbol<T> Get<T>(string name)
        {
            return null;
        }
    }
}
