using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

using rDSN.Tron.Contract;

namespace rDSN.Tron.Compiler
{
    public static class AliasHelper
    {
        

        //public static IValue<TSource> Assign<TSource>(
        //    this IValue<TSource> source,
        //    out IValue<TSource> var
        //    )
        //{
        //    var = source;
        //    return source;
        //}

        public class AssignPrimitiveAnalyzer : PrimitiveAnalyzer
        {
            public void Analysis(MethodCallExpression m, QueryContext context)
            {
                context.TempSymbolsByAlias.Add((m.Arguments[1] as MemberExpression).Member.Name, m.Arguments[0]);
                context.Visit(m.Arguments[0], 0);
            }
        }
    }
}
