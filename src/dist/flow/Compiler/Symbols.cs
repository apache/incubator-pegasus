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
