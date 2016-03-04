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
    public class QueryContext : ExpressionVisitor<object, int>
    {
        public static HashSet<string> KnownLibs = new HashSet<string>() { 
            "System.dll",
            "System.Core.dll",
            "rDSN.Tron.Utility.dll",
            "rDSN.Tron.Compiler.dll",
            "rDSN.Tron.Contract.dll",
            "rDSN.Tron.Runtime.Common.dll",
        };

        public Dictionary<Type, string> RewrittenTypes = new Dictionary<Type,string>(); // see TypeRewriter
        public Dictionary<string, string> ResourceLibraries = new Dictionary<string,string>(); // AllLibs - KnownLibs
        public Dictionary<string, string> AllLibraries = new Dictionary<string, string>(); //  

        public HashSet<Type> Types = new HashSet<Type>();
        public HashSet<MethodInfo> Methods = new HashSet<MethodInfo>();
        public Dictionary<MethodCallExpression, Service> ServiceCalls = new Dictionary<MethodCallExpression, Service>();
        public Dictionary<Expression, object> ExternalObjects = new Dictionary<Expression, object>();
        public Dictionary<ConstantExpression, ISymbol> InputSymbols = new Dictionary<ConstantExpression, ISymbol>();
        public Dictionary<ConstantExpression, object> InputConstants = new Dictionary<ConstantExpression, object>();
        public Dictionary<MemberExpression, Service> Services = new Dictionary<MemberExpression, Service>();
        public Dictionary<Expression, string> TempSymbols = new Dictionary<Expression, string>(); // var xyz = expression;
        public Dictionary<string, Expression> TempSymbolsByAlias = new Dictionary<string, Expression>();
        public Dictionary<MethodInfo, MethodCallExpression> ExternalComposedSerivce = new Dictionary<MethodInfo, MethodCallExpression>();

        public Type InputType { get; private set; }

        public Type OutputType { get; private set; }

        public MethodCallExpression RootExpression { get; private set; }

        public string Name { get; private set; }

        public object ComposedServiceObject { get; private set; }

        public QueryContext(object serviceObject, MethodCallExpression exp, string name = "Invoke")
        {
            ComposedServiceObject = serviceObject;
            RootExpression = exp;
            Name = name;
        }

        public void Collect()
        {
            Visit(RootExpression, 0);

            ExtractInputOutputTypes();
            ExtractAllAnonymousOrPrivateTypes();
            ExtractAllResources();
        }

        private void ExtractInputOutputTypes()
        {
            Trace.Assert(InputSymbols.Count >= 1);
            InputType = InputSymbols.First().Key.Type;
            OutputType = RootExpression.Type;
        }
                   
        private void ExtractAllAnonymousOrPrivateTypes()
        {
            Types.Where(t => t.IsAnonymous()).Select(t => { 
                RewrittenTypes.Add(t, "Anonymous_" + t.GetHashCode());
                return 0;
            }).Count();

            Types.Where(t => !t.IsAnonymous() && t.IsNotPublic).Select(t => {
                RewrittenTypes.Add(t, "NonPublic_" + t.Name + "_" + t.GetHashCode());
                return 0;
            }).Count();
        }

        private void ExtractAllResources()
        {
            Methods.Select(m =>
            {
                if (m.IsPublic == false)
                {
                    throw new Exception("Method call " + m.DeclaringType.FullName + "." + m.Name + " is not public therefore cannot be used in the query");
                }
                return 0;
            }).Count();

            var types = Types.Union(Methods.Select(mi => mi.DeclaringType))
                .Except(RewrittenTypes.Select(rt => rt.Key))
                .Where(t => !t.FullName.StartsWith("System."))
                .Distinct()
                .ToArray();

            var allModules = types
                .Select(t => t.Module)
                .Distinct()
                .ToArray()
                ;

            foreach (var m in allModules)
            {
                AllLibraries.Add(m.Name, m.FullyQualifiedName);
            }

            foreach (var m in allModules.Where(m => !KnownLibs.Contains(m.Name)))
            {
                ResourceLibraries.Add(m.Name, m.FullyQualifiedName);
            }
        }
        
        public override object VisitMethodCall(MethodCallExpression m, int nil)
        {
            var attrs = m.Method.GetCustomAttributes(typeof(Primitive), false).Cast<Primitive>().ToArray();
            if (attrs.Length > 0 && attrs[0].Analyzer != null)
            {
                attrs[0].Analyzer.Analysis(m, this);
                return null;
            }

            if (attrs.Length == 0 && (m.Type.IsSymbol() || m.Type.IsSymbols()))
            {
                var paramemters = new List<object>();
                int i = 0;

                foreach (var p in m.Method.GetParameters())
                {
                    i++;
                    if (p.ParameterType.IsSymbols())
                    {
                        var rp = p.ParameterType.GetConstructor(new Type[] { }).Invoke(new object[] { });
                        paramemters.Add(rp);
                    }
                    else
                    {
                        Trace.Assert(p.ParameterType.IsSymbol());
                        
                        var rp = p.ParameterType.GetConstructor(new Type[] { typeof(string) }).Invoke(new object[] { "p_" + i });

                        (rp as ISymbol).Name = "p_" + i;

                        paramemters.Add(rp);
                    }
                }

                var r = m.Method.Invoke(ComposedServiceObject, paramemters.ToArray());
                Trace.Assert(r.GetType().IsInheritedTypeOf(typeof(ISymbol)));

                var kexpr = (r as ISymbol).Expression;

                ExternalComposedSerivce.Add(m.Method, (r as ISymbol).Expression as MethodCallExpression);

                //Visit(kexpr, 0);
            }

            if (!Methods.Contains(m.Method))
                Methods.Add(m.Method);

            if (m.Object != null && m.Object.Type.IsInheritedTypeOf(typeof(Service)))
            {
                if (!ServiceCalls.ContainsKey(m))
                {
                    ServiceCalls.Add(m, GetValue(m.Object) as Service);
                }
            }

            return base.VisitMethodCall(m, nil);
        }

        public override object VisitConstant(ConstantExpression node, int nil)
        {
            object v;
            bool r = GetValue(node, out v);
            if (!r)
            {
                if (node.Type.IsGenericType && node.Type.BaseType == typeof(ISymbol))
                {
                    InputSymbols.Add(node, node.Value as ISymbol);
                }
                else
                {
                    throw new Exception("cannot resolve external constant expression '" + node + "'");
                }
            }
            else
            {
                if (node.Type.IsGenericType && node.Type.BaseType == typeof(ISymbol))
                {
                    InputSymbols.Add(node, node.Value as ISymbol);
                }
                else
                {
                    InputConstants.Add(node, r);
                }
            }
            return null;
        }

        public override object VisitMemberAccess(MemberExpression node, int nil)
        {
            string s;
            Object value;
            if (ExternalObjects.ContainsKey(node))
            {
                return null;
            }
            else if (GetValue(node, out value))
            {
                ExternalObjects[node] = value;
                if (node.Type.IsInheritedTypeOf(typeof(Service)))
                {
                    // TODO: multiple instances of the same type
                    bool t1 = Services.Keys.Select(k => k.Type.ToString()).Any(p => p == node.Type.ToString());
                    if (!t1)
                    {
                        Services.Add(node, value as Service);
                    }
                    return null;
                }
                else if (node.Type.IsInheritedTypeOf(typeof(Expression)))
                {
                    return null;
                }
                else if (node.Type.IsSymbol() || node.Type.IsSymbols())
                {
                    if (value == null)
                    {
                        Trace.Assert(TempSymbolsByAlias.ContainsKey(node.Member.Name), "a symbol must be first defined (e.g., using Alias operator)");
                    }
                    else
                    {
                        var kexpr = (value as ISymbol).Expression;
                        if (kexpr.NodeType != ExpressionType.Constant)
                        {
                            if (!TempSymbols.ContainsKey(kexpr))
                            {
                                TempSymbols.Add(kexpr, node.Member.Name);
                            }
                        }
                    }

                    return null;
                }

                bool r = LocalTypeHelper.ConstantValue2String(ExternalObjects[node], out s);
                if (r)
                {
                    return null;
                }
                else
                {
                    throw new Exception("cannot resolve external constant expression '" + node + "'");
                }
            }

            if (node.Expression != null)
            {
                this.Visit(node.Expression, nil);
            }
            return null;
        }

        public override object VisitNew(NewExpression nex, int param)
        {
            this.VisitExpressionList(nex.Arguments, param);

            Types.Add(nex.Type);

            return null;
        }

        public override object VisitNewArray(NewArrayExpression na, int param)
        {
            this.VisitExpressionList(na.Expressions, param);

            Types.Add(na.Type);

            return null;
        }

        public override object VisitInvocation(InvocationExpression iv, int param)
        {
            this.VisitExpressionList(iv.Arguments, param);
            this.Visit(iv.Expression, param);

            Types.Add(iv.Type);

            return null;
        }
    }
}
