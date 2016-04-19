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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.CSQL.Utility;

namespace Microsoft.CSQL.Contract
{
    public class ISymbol
    {
        public ISymbol(ICsqlQueryProvider provider = null, Expression exp = null, string name = "")
        {
            Provider = provider;
            Expression = exp;
            Name = name;

            if (null == Provider) Provider = new ICsqlQueryProvider();
            if (null == Expression) Expression = Expression.Constant(this);
        }

        public Expression Expression { get; private set; }
        public ICsqlQueryProvider Provider { get; private set; }
        public string Name { get; private set; }
    }

    public class ISymbol<TValue> : ISymbol
    {
        public ISymbol(string name)
            : base(null, null, name)
        {
            Value = default(TValue);
        }

        public ISymbol(ICsqlQueryProvider provider, Expression exp)
            : base(provider, exp, "")
        {
            Value = default(TValue);
        }

        public ISymbol(string name, TValue value)
            : base(null, null, name)
        {
            Value = value;
        }

        public TValue Value { get; set; }

    }

    public class ISymbols : ISymbol
    {
        public ISymbols(ICsqlQueryProvider provider = null, Expression exp = null)
            : base(provider, exp)
        {
        }
    }

    public class ISymbols<TElement> : ISymbols
    {
        public Type ElementType;

        public ISymbols(ICsqlQueryProvider provider = null, Expression exp = null)
            : base(provider, exp)
        {
            ElementType = typeof(TElement);
        }

        public TElement Value { get; set; }
    }
    
    public class ICsqlQueryProvider
    {
        public object Execute(Expression expression, bool IsSymbols)
        {
            //var serviceMesh = Compiler.Compile(expression, IsSymbols);

            throw new NotImplementedException();
        }

        //public ISymbols CreateSymbolsQuery(Expression expression)
        //{
        //    Type elementType = TypeHelper.GetElementType(expression.Type);
        //    try
        //    {
        //        return (ISymbols)Activator.CreateInstance(typeof(ISymbols<>).MakeGenericType(elementType), new object[] { this, expression });
        //    }
        //    catch (System.Reflection.TargetInvocationException tie)
        //    {
        //        throw tie.InnerException;
        //    }
        //}

        //public ISymbol CreateSymbolQuery(Expression expression)
        //{
        //    Type elementType = TypeHelper.GetElementType(expression.Type);
        //    try
        //    {
        //        return (ISymbol)Activator.CreateInstance(typeof(ISymbol<>).MakeGenericType(elementType), new object[] { this, expression });
        //    }
        //    catch (System.Reflection.TargetInvocationException tie)
        //    {
        //        throw tie.InnerException;
        //    }
        //}

        public ISymbols<TResult> CreateSymbolsQuery<TResult>(Expression expression)
        {
            return new ISymbols<TResult>(this, expression);
        }

        public ISymbol<TResult> CreateSymbolQuery<TResult>(Expression expression)
        {
            return new ISymbol<TResult>(this, expression);
        }

        public object Execute(Expression expression)
        {
            return Execute(expression, false);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression, typeof(TResult).IsSymbols());
        }
    }
}
