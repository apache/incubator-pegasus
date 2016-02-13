using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

namespace rDSN.Tron.Compiler
{
    [System.AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class Primitive : System.Attribute
    {
        public Primitive()
        {
            Analyzer = null;
        }

        public Type AnalyzerType
        {
            get
            {
                if (Analyzer == null)
                    return null;
                else
                    return Analyzer.GetType();
            }
            set
            {
                if (value != null)
                {
                    Analyzer = value.GetConstructor(new Type[] { }).Invoke(new object[] { }) as PrimitiveAnalyzer;
                }
            }
        }

        public PrimitiveAnalyzer Analyzer { get; private set; }
    }
    
    public interface PrimitiveAnalyzer
    {
        void Analysis(MethodCallExpression m, QueryContext context);
    }
}
