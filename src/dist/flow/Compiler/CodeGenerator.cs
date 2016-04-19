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
using System.Reflection;
using rDSN.Tron.Contract;
using rDSN.Tron.LanguageProvider;
using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    //
    // build compilable query
    //  all external values must be converted into constant
    //  all external functions and types must be referenced with full namespace
    //    
    //
    public class CodeGenerator
    {
        private CodeBuilder _builder = new CodeBuilder();
        private QueryContext[] _contexts;
        private string _appClassName;
        private Dictionary<Type, string> _rewrittenTypes = new Dictionary<Type, string>();

        public ulong AppId { get; } = RandomGenerator.Random64();

        public string BuildRdsn(Type service, QueryContext[] contexts)
        {
            //_stages = stages;
            _contexts = contexts;
            _appClassName = service.Name;

            //BuildInputOutputValueTypes();
            BuildHeaderRdsn(service.Namespace);
            BuildRewrittenTypes();
            _builder.AppendLine("public class " + _appClassName + "Server_impl :" + _appClassName + "Server");
            _builder.BeginBlock();
            BuildServiceClientsRdsn();
            //thrift or protobuf
            BuildServiceCallsRdsn(_appClassName);
            foreach (var c in contexts)
                //never change
                BuildQueryRdsn(c);

            //always thrift
            BuildServer(_appClassName, ServiceContract.GetServiceCalls(service));

            _builder.EndBlock();

            BuildMain();
            BuildFooter();
            return _builder.ToString();
        }
        public void BuildMain()
        {
            _builder.AppendLine("class Program");
            _builder.BeginBlock();
            _builder.AppendLine("static void Main(string[] args)");
            _builder.BeginBlock();
            _builder.AppendLine(_appClassName + "Helper.InitCodes();");
            foreach (var s in _contexts.SelectMany(c => c.Services).DistinctBy(s => s.Key.Member.Name))
            {
                _builder.AppendLine(s.Value.Spec.MainSpecFile.Split('.')[0] + "Helper.InitCodes();");
            }
            _builder.AppendLine("ServiceApp.RegisterApp<" + _appClassName + "ServerApp>(\"server\");");
            _builder.AppendLine("ServiceApp.RegisterApp<" + _appClassName + "ClientApp>(\"client\");");
            _builder.AppendLine("string[] args2 = (new string[] { \"" + _appClassName + "\" }).Union(args).ToArray();");
            _builder.AppendLine("Native.dsn_run(args2.Length, args2, true);");
            _builder.EndBlock();
            _builder.EndBlock();
        }
        
        public string Build(string className, QueryContext[] contexts)
        {
            //_stages = stages;
            _contexts = contexts;
            _appClassName = className;

            //BuildInputOutputValueTypes();
            BuildRewrittenTypes();                       

            BuildHeader();

            _builder.AppendLine("public class " + _appClassName + " : ServiceMesh");
            _builder.BeginBlock();

            //BuildConstructor();
            BuildServiceClients();
            BuildServiceCalls();
            foreach (var c in contexts)
                BuildQuery(c);

            
            _builder.EndBlock();
            
            BuildFooter();
            return _builder.ToString();
        }
        private void BuildConstructor()
        {
            _builder.AppendLine("public " + _appClassName + "()");
            _builder.BeginBlock();

            _builder.EndBlock();
            _builder.AppendLine();
        }

        //private void BuildInputOutputValueTypes()
        //{
        //    if (_primaryContext.OutputType.IsSymbols())
        //    {
        //        throw new Exception("we are not support ISymbolCollection<> output right now, you can use an Gather method to merge it into a single ISymbol<>");
        //    }

        //    Trace.Assert(_primaryContext.InputType.IsSymbol() && _primaryContext.InputType.IsGenericType);

        //    Trace.Assert(_primaryContext.OutputType.IsSymbol() && _primaryContext.OutputType.IsGenericType);

        //    _inputValueType = _primaryContext.InputType.GetGenericArguments()[0];
        //    _outputValueType = _primaryContext.OutputType.GetGenericArguments()[0];

        //}

        private void BuildServiceClientsRdsn()
        {
            foreach (var s in _contexts.SelectMany(c => c.Services).DistinctBy(s => s.Key.Member.Name))
            {
                _builder.AppendLine("private " + s.Value.TypeName() + "Client " + s.Key.Member.Name + " = new " + s.Value.TypeName() + "Client(new RpcAddress(\"" + s.Value.URL + "\"));");
                _builder.AppendLine();
            }
        }
        
        private void BuildServiceClients()
        {
           
        }

        private void BuildServiceCallsRdsn(string serviceName)
        {
            var calls = new HashSet<string>();
            foreach (var s in _contexts.SelectMany(c => c.ServiceCalls))
            {
                Trace.Assert(s.Key.Object != null && s.Key.Object.NodeType == ExpressionType.MemberAccess);
                var callName = s.Key.Method.Name;
                var respTypeName = s.Key.Type.GetCompilableTypeName(_rewrittenTypes);
                var reqTypeName = s.Key.Arguments[0].Type.GetCompilableTypeName(_rewrittenTypes);
                var call = "Call_" + s.Value.PlainTypeName() + "_" + callName;

                if (!calls.Add(call + ":" + reqTypeName))
                    continue;
                _builder.AppendLine("private " + respTypeName + " " + call + "( " + reqTypeName + " req)");
                _builder.BeginBlock();
                var provider = SpecProviderManager.Instance().GetProvider(s.Value.Spec.SType);
                provider.GenerateClientCall(_builder, s.Key, s.Value, _rewrittenTypes);
                _builder.EndBlock();
                _builder.AppendLine();
            }
        }

        private void BuildServiceCalls()
        {
            
        }

        private void BuildQueryRdsn(QueryContext c)
        {
            _builder.AppendLine("public " + c.OutputType.GetGenericArguments()[0].FullName.GetCompilableTypeName()
                        + " " + c.Name + "(" + c.InputType.GetCompilableTypeName(_rewrittenTypes) + " request)");

            _builder.AppendLine("{");
            _builder++;

            _builder.AppendLine("Console.Write(\".\");");

            // local vars
            foreach (var s in c.TempSymbolsByAlias)
            {
                _builder.AppendLine(s.Value.Type.GetCompilableTypeName(_rewrittenTypes) + " " + s.Key + ";");
            }

            if (c.TempSymbolsByAlias.Count > 0)
                _builder.AppendLine();

            // final query
            var codeBuilder = new ExpressionToCode(c.RootExpression, c);
            var code = codeBuilder.GenCode(_builder.Indent);

            _builder.AppendLine(code + ";");

            _builder--;
            _builder.AppendLine("}");
            _builder.AppendLine();

        }

        private void BuildServer(string serviceName, MethodInfo[] methods)
        {
            foreach (var m in methods)
            {
                var respType = serviceName + "." + m.Name + "_result";
                _builder.AppendLine("protected override void On" + m.Name + "(" + serviceName + "." + m.Name + "_args request, RpcReplier<" + respType + "> replier)");
                _builder.BeginBlock();
                _builder.AppendLine("var resp = new " + respType + "();");
                _builder.AppendLine("resp.Success = " + m.Name + "(new IValue<" + m.GetParameters()[0].ParameterType.GetGenericArguments()[0].FullName.GetCompilableTypeName() + ">(request.Req));");
                _builder.AppendLine("replier.Reply(resp);");
                _builder.EndBlock();
                _builder.AppendLine();
            }
        }


        private void BuildQuery(QueryContext c)
        {
            _builder.AppendLine("public " + c.OutputType.GetCompilableTypeName(_rewrittenTypes)
                        + " " + c.Name + "(" + c.InputType.GetCompilableTypeName(_rewrittenTypes) + " request)");
            
            _builder.AppendLine("{");
            _builder++;

            _builder.AppendLine("Console.Write(\".\");");

            // local vars
            foreach (var s in c.TempSymbolsByAlias)
            {
                _builder.AppendLine(s.Value.Type.GetCompilableTypeName(_rewrittenTypes) + " " + s.Key + ";");
            }

            if (c.TempSymbolsByAlias.Count > 0)
                _builder.AppendLine();

            // final query
            var codeBuilder = new ExpressionToCode(c.RootExpression, c);
            var code = codeBuilder.GenCode(_builder.Indent);

            _builder.AppendLine(code + ";");

            _builder--;
            _builder.AppendLine("}");
            _builder.AppendLine();
        }
        
        private string VerboseStringArray(string[] parameters)
        {
            var ps = parameters.Aggregate("", (current, s) => current + ("@\"" + s + "\","));
            if (ps.Length > 0)
            {
                ps = ps.Substring(0, ps.Length - 1);
            }
            return ps;
        }
        
        private void BuildRewrittenTypes()
        {
            foreach (var t in _contexts.SelectMany(c => c.RewrittenTypes.Where(t => !_rewrittenTypes.ContainsKey(t.Key))))
            {
                _rewrittenTypes.Add(t.Key, t.Value);
            }

            foreach (var c in _contexts)
            {
                c.RewrittenTypes = _rewrittenTypes;
            }

            foreach (var typeMap in _rewrittenTypes)
            {
                _builder.AppendLine("class " + typeMap.Value);
                _builder.AppendLine("{");
                _builder++;

                foreach (var property in typeMap.Key.GetProperties())
                {
                    _builder.AppendLine("public " + property.PropertyType.GetCompilableTypeName(_rewrittenTypes) + " " + property.Name + " { get; set; }");
                }

                _builder.AppendLine("public " + typeMap.Value + " () {}");

                _builder--;
                _builder.AppendLine("}");
                _builder.AppendLine();
            }
        }

        private void BuildHeaderRdsn(string serviceNamespce)
        {

            _builder.AppendLine("/* AUTO GENERATED BY Tron AT " + DateTime.Now.ToLocalTime() + " */");


            var namespaces = new HashSet<string>
            {
                "System",
                "System.IO",
                "dsn.dev.csharp",
                serviceNamespce,
                "System.Linq",
                "System.Text",
                "System.Linq.Expressions",
                "System.Reflection",
                "System.Diagnostics",
                "System.Net",
                "System.Threading",
                "rDSN.Tron.Contract",
                "rDSN.Tron.Runtime",
                "rDSN.Tron.App"
            };

            //namespaces.Add("rDSN.Tron.Utility");
            //namespaces.Add("rDSN.Tron.Compiler");

            foreach (var nm in _contexts.SelectMany(c => c.Methods).Select(mi => mi.DeclaringType.Namespace).Distinct().Except(namespaces))
            {
                namespaces.Add(nm);
            }

            foreach (var np in namespaces)
            {
                _builder.AppendLine("using " + np + ";");
            }

            _builder.AppendLine();

            _builder.AppendLine("namespace rDSN.Tron.App");
            _builder.AppendLine("{");
            _builder++;
        }
        private void BuildHeader()
        {
            _builder.AppendLine("/* AUTO GENERATED BY Tron AT " + DateTime.Now.ToLocalTime() + " */");


            var namespaces = new HashSet<string>
            {
                "System",
                "System.IO",
                "System.Collections.Generic",
                "System.Linq",
                "System.Text",
                "System.Linq.Expressions",
                "System.Reflection",
                "System.Diagnostics",
                "System.Net",
                "System.Threading",
                "rDSN.Tron.Utility",
                "rDSN.Tron.Contract",
                "rDSN.Tron.Runtime"
            };

            //namespaces.Add("rDSN.Tron.Compiler");

            foreach (var nm in _contexts.SelectMany(c => c.Methods).Select(mi => mi.DeclaringType.Namespace).Distinct().Except(namespaces))
            {
                namespaces.Add(nm);
            }
            
            foreach (var np in namespaces)
            {
                _builder.AppendLine("using " + np + ";");
            }

            _builder.AppendLine();

            _builder.AppendLine("namespace rDSN.Tron.App");
            _builder.AppendLine("{");
            _builder++;
        }

        private void BuildFooter()
        {
            _builder--;
            _builder.AppendLine("} // end namespace");
            _builder.AppendLine();
        }

    }
}
