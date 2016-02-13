using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace rDSN.Tron.Compiler
{
    public static class ServiceModifiers
    {
        public static ISymbol<T> Batch<T>(this ISymbol<T> source) // TODO: batch parameters
        {
            return source;
        }

        // partition, replication to services

        public static ISymbol<T> SLAControl<T>(this ISymbol<T> source) // TODO: SLA parameters
        {
            return source;
        }
    }
}
