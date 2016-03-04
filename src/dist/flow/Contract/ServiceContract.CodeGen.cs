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
using System.Threading.Tasks;
using System.Reflection;
using System.Diagnostics;

using rDSN.Tron.Utility;

namespace rDSN.Tron.Contract
{
    public partial class ServiceContract
    {
        public static string GenerateComposedServiceImplementation(Type type)
        {
            CodeBuilder builder = new CodeBuilder();

            builder.AppendLine("using System;");
            builder.AppendLine("using System.Collections.Generic;");
            builder.AppendLine("using System.Linq;");
            builder.AppendLine("using System.Text;");
            builder.AppendLine("using System.Threading.Tasks;");
            builder.AppendLine("using System.Threading;");
            builder.AppendLine("using System.Collections.Concurrent;");
            builder.AppendLine("using System.Diagnostics;");
            builder.AppendLine("using System.IO;");
            builder.AppendLine();
            builder.AppendLine("using BondNetlibTransport;");
            builder.AppendLine("using BondTransport;");
            builder.AppendLine("using Microsoft.Bond;");
            builder.AppendLine("using rDSN.Tron.Utility;");
            builder.AppendLine("using rDSN.Tron.Contract;");
            builder.AppendLine("using rDSN.Tron.Runtime;");
            builder.AppendLine();

            // namespace
            builder.AppendLine("namespace " + type.Namespace);
            builder.BeginBlock();

            // serivce impl
            builder.AppendLine("public class " + type.Name + "_ServiceWrapper : " + type.Name + "_Service");
            builder.BeginBlock();

            // internal composed service
            builder.AppendLine("private " + type.Name + " _svc = new " + type.Name + "();");
            builder.AppendLine();

            // ctor
            builder.AppendLine("public " + type.Name + "_ServiceWrapper()");
            builder.BeginBlock();
            builder.AppendLine("_svc.Start();");
            builder.EndBlock();
            builder.AppendLine();

            // serivce calls
            foreach (var m in ServiceContract.GetServiceCalls(type))
            {
                string responseType = m.ReturnType.GetGenericArguments()[0].FullName.GetCompilableTypeName();
                string requestType = m.GetParameters()[0].ParameterType.GetGenericArguments()[0].FullName.GetCompilableTypeName();

                builder.AppendLine("public override void " + m.Name  + "(Request<" + requestType + ", " + responseType + "> call)");
                builder.BeginBlock();

                builder.AppendLine("var resp = _svc." + m.Name + "(new IValue<" + requestType + ">(call.RequestObject));");
                builder.AppendLine("call.Dispatch(resp.Value());");

                builder.EndBlock();
                builder.AppendLine();
            }
            

            // end service impl
            builder.EndBlock();
            // end namespace
            builder.EndBlock();

            return builder.ToString();
        }
        public static string GenerateThriftSpec(Type type, List<string> dependentSpecFiles)
        {
            var thriftTypeMapping = new Dictionary<Type, string>()
            {
                {typeof(bool), "bool"},
                {typeof(byte), "byte"},
                {typeof(Int16), "i16" },
                {typeof(Int32), "i32"},
                {typeof(Int64), "i64" },
                {typeof(double), "double" },
                {typeof(byte[]), "binary"},
                {typeof(string), "string" }
            };
            CodeBuilder builder = new CodeBuilder();
            
            builder.AppendLine();
            builder.AppendLine("namespace csharp " + type.Namespace.ToString());
            builder.AppendLine("service " + type.Name);
            builder.BeginBlock();

            foreach (var m in ServiceContract.GetServiceCalls(type))
            {
                var return_value_type = m.ReturnType.GetGenericArguments()[0];
                var parameter_type = m.GetParameters()[0].ParameterType.GetGenericArguments()[0];
                string return_value_name = thriftTypeMapping.ContainsKey(return_value_type) ? thriftTypeMapping[return_value_type] : return_value_type.FullName.GetCompilableTypeName();
                string parameter_name = thriftTypeMapping.ContainsKey(parameter_type) ? thriftTypeMapping[parameter_type] : parameter_type.FullName.GetCompilableTypeName();
                builder.AppendLine(return_value_name + " " + m.Name + "(" + parameter_name + " req);");
            }

            builder.EndBlock();
            builder.AppendLine();

            return builder.ToString();
        }

        public static string GenerateBondSpec(Type type, List<string> dependentSpecFiles)
        {
            CodeBuilder builder = new CodeBuilder();

            foreach (var s in dependentSpecFiles)
            {
                builder.AppendLine("import \"" + s + "\"");
            }

            builder.AppendLine();
            builder.AppendLine("namespace " + type.Namespace.ToString());
            builder.AppendLine("service " + type.Name);
            builder.BeginBlock();

            foreach (var m in ServiceContract.GetServiceCalls(type))
            {
                builder.AppendLine(m.ReturnType.GetGenericArguments()[0].FullName.GetCompilableTypeName()
                    + " " + m.Name + "(" +
                    m.GetParameters()[0].ParameterType.GetGenericArguments()[0].FullName.GetCompilableTypeName()
                    + " request);");
            }

            builder.EndBlock();
            builder.AppendLine();

            return builder.ToString();
        }

        public static void GenerateCompositionStub(Type iface, ServicePackage package, CodeBuilder cb)
        {
            var props = GetProps(iface);

            cb.AppendLine();

            cb.AppendLine("public sealed class Service_" + iface.Name);
            cb.AppendLine("\t : rDSN.Tron.Contract.Service");
            cb.BeginBlock();

            cb.AppendLine("public Service_" + iface.Name + "(string serviceUri, string name = \"\") : base(typeof(" + iface.FullName + "), \"" + package.Name + "\", serviceUri, name)");
            cb.BeginBlock();

            foreach (var prop in props.GetType().GetProperties())
            {
                var val = prop.GetValue(props);
                cb.AppendLine("Properties." + prop.Name + " = " + (val == null ? "null" : LocalTypeHelper.ConstantValue2StringInternal(prop.GetValue(props))) + ";");
            }

            cb.AppendLine("Spec.SType = ServiceSpecType." + package.Spec.SType.ToString() + ";");
            cb.AppendLine("Spec.MainSpecFile = \"" + package.Spec.MainSpecFile + "\";");
            cb.AppendLine("Spec.ReferencedSpecFiles = new List<string>();");
            cb.AppendLine("Spec.IsRdsnRpc = " + (package.Spec.IsRdsnRpc ? "true" : "false") + ";");
            foreach (var p in package.Spec.ReferencedSpecFiles)
            {
                cb.AppendLine("Spec.ReferencedSpecFiles.Add(\"" + p + "\");");
            }

            cb.EndBlock();
            cb.AppendLine();

            foreach (var m in GetServiceCalls(iface))
            {
                if (m.DeclaringType.IsInterface)
                {
                    cb.AppendLine("public " + m.ReturnType.FullName + " " + m.Name + "(" + m.GetParameters().VerboseCombine(", ", p => p.ParameterType.FullName + " " + p.Name) + ")");
                }
                else
                {
                    cb.AppendLine("public " + m.ReturnType.GetGenericArguments()[0].FullName + " " + m.Name + "(" + m.GetParameters().VerboseCombine(", ", p => p.ParameterType.GetGenericArguments()[0].FullName + " " + p.Name) + ")");
                }

                cb.BeginBlock();
                cb.AppendLine("throw new NotImplementedException(\"no need to implement, this is just a placeholder\");");
                cb.EndBlock();
                cb.AppendLine();
            }

            var upcalls = iface.GetMethods().Where(im => IsUpCall(im)).ToArray();
            if (upcalls.Length > 0)
            {
                foreach (var m in upcalls)
                {
                    Trace.Assert(m.GetParameters().Length == 1, "upcalls must have one and only one parameter");
                    cb.AppendLine("ISymbolStream<" + m.GetParameters()[0].ParameterType.FullName + "> StreamOf_" + m.Name + " = new ISymbolStream<" + m.GetParameters()[0].ParameterType.FullName + ">();");
                    cb.AppendLine();
                }
            }

            cb.EndBlock();
        }

        public static bool IsServiceCall(MethodInfo m)
        {
            return m.GetParameters().Length == 1
                   && !IsUpCall(m)
                   && (m.DeclaringType.IsInterface || (
                    m.GetParameters()[0].ParameterType.IsSymbol()
                    && m.ReturnType.IsSymbol()
                    // TODO: symbolcollections etc.
                   ))
                   ;
        }

        public static MethodInfo[] GetServiceCalls(Type service)
        {
            return service.GetMethods().Where(m => IsServiceCall(m)).ToArray();
        }

        public static bool IsUpCall(MethodInfo m)
        {
            var attrs = m.GetCustomAttributes(typeof(UpCall), false).Cast<UpCall>().ToArray();
            return attrs.Length > 0;
        }

        public static bool HasSideEffect(MethodInfo m)
        {
            var attrs = m.GetCustomAttributes(typeof(SideEffect), false).Cast<SideEffect>().ToArray();
            return attrs.Length > 0;
        }

        public static bool IsComposed(MethodInfo m)
        {
            var attrs = m.GetCustomAttributes(typeof(Composed), false).Cast<Composed>().ToArray();
            return attrs.Length > 0;
        }

        public static bool IsTronService(Type iface)
        {
            var attrs = iface.GetCustomAttributes(typeof(TronService), false).Cast<TronService>().ToArray();
            return attrs.Length > 0;
        }

        public static ServiceProperty GetProps(Type iface)
        {
            var attrs = iface.GetCustomAttributes(typeof(TronService), false).Cast<TronService>().ToArray();
            if (attrs.Length > 0)
            {
                return attrs[0].Props;
            }
            else
            {
                throw new Exception("type '" + iface.FullName + "' is not a tron service, please use [TronService] annotation if it is.");
            }
        }
    }
}
