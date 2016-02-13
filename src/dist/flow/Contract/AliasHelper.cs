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
