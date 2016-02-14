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
 
////////////////////////////////////////////////////////////////////////////////
// Copyright (c) Microsoft Corporation.  All Rights Reserved.
//
// File Name: Symbols.cs
//
// Description:
//
//   Variable representation in service composition language
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    public class ISymbol
    {
        public ISymbol(Expression exp = null, string name = "")
        {
            Expression = exp;
            Name = name;

            if (null == Expression) Expression = Expression.Constant(this);
        }

        public Expression Expression { get; private set; }
        public string Name { get; set; }

        public ISymbolCollection<TResult> CreateCollectionQuery<TResult>(Expression expression)
        {
            return new ISymbolCollection<TResult>(expression);
        }

        public ISymbol<TResult> CreateQuery<TResult>(Expression expression)
        {
            return new ISymbol<TResult>(expression);
        }
    }

    public class ISymbol<TValue> : ISymbol
    {
        public ISymbol(string name)
            : base(null, name)
        {
        }

        public ISymbol(Expression exp)
            : base(exp, "")
        {
        }

        public TValue Value()
        {
            throw new NotImplementedException("impossible execution flow");
        }
    }

    public class ISymbolCollection : ISymbol
    {
        public ISymbolCollection(Expression exp = null)
            : base(exp)
        {
        }
    }

    public class ISymbolCollection<TElement> : ISymbolCollection
    {
        public ISymbolCollection(Expression exp = null)
            : base(exp)
        {
        }

        public IEnumerable<TElement> AsEnumerable()
        {
            throw new NotImplementedException("impossible execution flow");
        }
    }

    public class ISymbolStream<TElement> : ISymbolCollection<TElement>
    {
        public ISymbolStream(Expression exp = null)
            : base(exp)
        {
        }
    }
}
