using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Dynamic;
using System.Globalization;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.Compiler
{
    internal class PrimitiveGraphBuilder : ExpressionVisitor<object, int>
    {
        private MethodCallExpression _rootCall;
        private LGraph _graph;
        private Dictionary<MethodCallExpression, LVertex> _vertices = new Dictionary<MethodCallExpression, LVertex>();
        private Stack<LVertex> _currentParentVertices = new Stack<LVertex>();
        private object _composedService = null;

        internal PrimitiveGraphBuilder(object serviceObject, MethodCallExpression m, LGraph graph)
        {
            Trace.Assert(IsTronPrimitiveCall(m));

            _composedService = serviceObject;
            _rootCall = m;
            _graph = graph;
        }

        internal LVertex Build()
        {
            return Visit(_rootCall, 0) as LVertex;
        }

        internal bool IsTronPrimitiveCall(MethodCallExpression m)
        {
            var attrs = m.Method.GetCustomAttributes(typeof(Primitive), false).Cast<Primitive>().ToArray();
            return attrs.Length > 0;
        }

        internal bool IsTronComposedCall(MethodCallExpression m)
        {
            return m.Object != null
                && m.Method.GetParameters().Length == 1
                && (m.Method.GetParameters()[0].ParameterType.IsSymbol() || m.Method.GetParameters()[0].ParameterType.IsSymbols())
                && (m.Method.ReturnType.IsSymbol() || m.Method.ReturnType.IsSymbols())
                ;
        }

        //private LVertex VisitPrimtiveMethodCall(MethodCallExpression m, int scopeLevel)
        //{ 
        
        //}

        //private LVertex VisitComposedMethodCall(MethodCallExpression m, int scopeLevel)
        //{
            
        //}
        
        public override object VisitMethodCall(MethodCallExpression m, int scopeLevel)
        {
            Trace.Assert(IsTronPrimitiveCall(m) || IsTronComposedCall(m));

            if (_vertices.ContainsKey(m))
                return _vertices[m];

            LVertex v = _graph.CreateVertex<LVertex>();
            v.Exp = m;
            v.Name = m.Method.Name + "(" + scopeLevel + ")";
            _vertices.Add(m, v);
            
            Expression source = null;
            if (m.Object != null)
            {
                source = m.Object;
            }
            else
            {
                source = m.Arguments[0];
            }

            if (source.NodeType == ExpressionType.Constant)
            {
                ConstantExpression cexp = source as ConstantExpression;
                if (cexp.Value.GetType().IsInheritedTypeOf(typeof(ISymbol)))
                {
                    var input = _graph.CreateVertex<LVertex>();
                    input.Name = "input";

                    var ie = input.ConnectTo<LEdge>(v);
                    ie.Type = LEdge.FlowType.Data;
                }
                else
                {
                    throw new Exception("input variable not allowed for ' " + cexp.Value.GetType() + "' in exp '" + m.ToString() + "'");
                }
            }
            else if (source.NodeType == ExpressionType.Parameter)
            {
                var input = _graph.CreateVertex<LVertex>();
                input.Name = (source as ParameterExpression).Name;

                var ie = input.ConnectTo<LEdge>(v);
                ie.Type = LEdge.FlowType.Data;
            }
            else if (source is MethodCallExpression)
            {
                var sv = this.Visit(source, scopeLevel) as LVertex;
                var ie = sv.ConnectTo<LEdge>(v);
                ie.Type = LEdge.FlowType.Data;
            }
            else
            {
                throw new Exception("Invalid source expression: " + source.ToString());
            }

            foreach (var arg in m.Arguments)
            {
                if (arg == source)
                    continue;

                if (arg.NodeType == ExpressionType.Quote)
                {
                    UnaryExpression uexp = arg as UnaryExpression;
                    if (uexp.Operand.NodeType == ExpressionType.Lambda)
                    {
                        LambdaExpression lexp = uexp.Operand as LambdaExpression;
                        if (lexp.Parameters.Count > 0 && lexp.Parameters[0].Type.IsSymbol() && lexp.Body.NodeType == ExpressionType.Call)
                        {
                            var cexp = lexp.Body as MethodCallExpression;
                            if (IsTronPrimitiveCall(cexp))
                            {
                                var sv = Visit(lexp.Body, scopeLevel + 1) as LVertex;
                                var ie = sv.ConnectTo<LEdge>(v);
                                ie.Type = LEdge.FlowType.Lambda;
                            }

                            else
                            {
                                Trace.Assert(IsTronComposedCall(cexp));
                                var req = cexp.Method.GetParameters()[0].ParameterType.GetConstructor(new Type[] { typeof(string) }).Invoke(new object[] { "request" });
                                var result = (ISymbol)cexp.Method.Invoke(_composedService, new object[] { req });

                                LVertex sv = Visit(result.Expression, scopeLevel + 1) as LVertex;
                                var ie = sv.ConnectTo<LEdge>(v);
                                ie.Type = LEdge.FlowType.Lambda;
                            }
                        }
                    }
                }
            }

            return v;
        }        
    }
}
