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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    internal class PrimitiveGraphBuilder : ExpressionVisitor<object, int>
    {
        private MethodCallExpression _rootCall;
        private LGraph _graph;
        private Dictionary<MethodCallExpression, LVertex> _vertices = new Dictionary<MethodCallExpression, LVertex>();
        private Stack<LVertex> _currentParentVertices = new Stack<LVertex>();
        private object _composedService;

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

            var v = _graph.CreateVertex<LVertex>();
            v.Exp = m;
            v.Name = m.Method.Name + "(" + scopeLevel + ")";
            _vertices.Add(m, v);

            var source = m.Object ?? m.Arguments[0];

            if (source.NodeType == ExpressionType.Constant)
            {
                var cexp = source as ConstantExpression;
                if (cexp.Value.GetType().IsInheritedTypeOf(typeof(ISymbol)))
                {
                    var input = _graph.CreateVertex<LVertex>();
                    input.Name = "input";

                    var ie = input.ConnectTo<LEdge>(v);
                    ie.Type = LEdge.FlowType.Data;
                }
                else
                {
                    throw new Exception("input variable not allowed for ' " + cexp.Value.GetType() + "' in exp '" + m + "'");
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
                var sv = Visit(source, scopeLevel) as LVertex;
                var ie = sv.ConnectTo<LEdge>(v);
                ie.Type = LEdge.FlowType.Data;
            }
            else
            {
                throw new Exception("Invalid source expression: " + source);
            }

            foreach (var arg in m.Arguments)
            {
                if (arg == source)
                    continue;

                if (arg.NodeType != ExpressionType.Quote) continue;
                var uexp = arg as UnaryExpression;
                if (uexp.Operand.NodeType != ExpressionType.Lambda) continue;
                var lexp = uexp.Operand as LambdaExpression;
                if (lexp.Parameters.Count <= 0 || !lexp.Parameters[0].Type.IsSymbol() ||
                    lexp.Body.NodeType != ExpressionType.Call) continue;
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
                    var req = cexp.Method.GetParameters()[0].ParameterType.GetConstructor(new[] { typeof(string) }).Invoke(new object[] { "request" });
                    var result = (ISymbol)cexp.Method.Invoke(_composedService, new[] { req });

                    var sv = Visit(result.Expression, scopeLevel + 1) as LVertex;
                    var ie = sv.ConnectTo<LEdge>(v);
                    ie.Type = LEdge.FlowType.Lambda;
                }
            }

            return v;
        }        
    }
}
