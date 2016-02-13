using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    public class OperatorMgr
    {
        public static void Register(OperatorTransformer transfomer)
        {
            _transformers.Add(transfomer.Method, transfomer);
        }

        public static OperatorTransformer Get(MethodBase method)
        {
            OperatorTransformer transformer = null;
            _transformers.TryGetValue(method, out transformer);
            return transformer;
        }

        private static Dictionary<MethodBase, OperatorTransformer> _transformers = new Dictionary<MethodBase, OperatorTransformer>(); 
    }

    public abstract class OperatorTransformer
    {
        public OperatorTransformer(MethodBase method)
        {
            Method = method;
        }

        public MethodBase Method { get; private set; }

        public abstract void TransformGraph(LVertex v);

        public abstract void GenerateCode(LVertex v, CodeBuilder builder);
    }
}
