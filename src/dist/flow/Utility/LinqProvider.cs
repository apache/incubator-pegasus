using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace rDSN.Tron.Utility
{
    public class QueryableObjects<T> : IOrderedQueryable<T>
    {
        #region Constructors
        /// <summary>
        /// This constructor is called by the client to create the data source.
        /// </summary>
        public QueryableObjects(QueryProvider provider)
        {
            Provider = provider;
            Expression = Expression.Constant(this);
        }

        /// <summary>
        /// This constructor is called by Provider.CreateQuery().
        /// </summary>
        /// <param name="expression"></param>
        public QueryableObjects(QueryProvider provider, Expression expression)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }

            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("expression");
            }

            Provider = provider;
            Expression = expression;
        }
        #endregion

        #region Properties

        public IQueryProvider Provider { get; private set; }
        public Expression Expression { get; private set; }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        #endregion

        #region Enumerators
        public IEnumerator<T> GetEnumerator()
        {
            return (Provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (Provider.Execute<IEnumerable>(Expression)).GetEnumerator();
        }
        #endregion
    }

    public abstract class QueryProvider : IQueryProvider
    {
        public abstract object Execute(Expression expression, bool IsSymbols);

        public IQueryable CreateQuery(Expression expression)
        {
            Type elementType = TypeHelper.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(QueryableObjects<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (System.Reflection.TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public IQueryable<TResult> CreateQuery<TResult>(Expression expression)
        {
            return new QueryableObjects<TResult>(this, expression);
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
