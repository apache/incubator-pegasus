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
