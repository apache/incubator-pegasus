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
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using rDSN.Tron.Contract;
using rDSN.Tron.LanguageProvider;
using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    public static class Compiler
    {
        public static ServicePlan Compile(Type serviceType)
        {
            var calls = new Dictionary<MethodInfo, MethodCallExpression>();
            var ctor = serviceType.GetConstructor(new Type[] { });
            Debug.Assert(ctor != null, "ctor != null");
            var this_ = ctor.Invoke(new object[] { });

            foreach (var m in ServiceContract.GetServiceCalls(serviceType))
            {
                var constructorInfo = m.GetParameters()[0].ParameterType.GetConstructor(new[] { typeof(string) });
                if (constructorInfo == null) continue;
                var req = constructorInfo.Invoke(new object[] { "request" });
                var result = (ISymbol)m.Invoke(this_, new[] { req });

                calls[m] = result.Expression as MethodCallExpression;
            }

            return Compile(calls, this_);
        }

        private static ServicePlan Compile(Dictionary<MethodInfo, MethodCallExpression> expressions, object serviceObject)
        {
            // step: collect all types, methods, variables referenced in this expression
            var workerQueue = new Queue<KeyValuePair<MethodInfo, MethodCallExpression>>();

            foreach (var exp in expressions)
            {
                workerQueue.Enqueue(new KeyValuePair<MethodInfo, MethodCallExpression>(exp.Key, exp.Value));
            }

            var contexts = new Dictionary<MethodInfo, QueryContext>();
            while (workerQueue.Count > 0)
            {
                var exp = workerQueue.Dequeue();
                var context = new QueryContext(serviceObject, exp.Value, exp.Key.Name);
                context.Collect();
                contexts.Add(exp.Key, context);
                foreach (var s in context.ExternalComposedSerivce.Where(s => !contexts.ContainsKey(s.Key)))
                {
                    workerQueue.Enqueue(new KeyValuePair<MethodInfo, MethodCallExpression>(s.Key, s.Value));
                }
            }

            // step: prepare service plan
            var codeGenerator = new CodeGenerator();
            var name = serviceObject.GetType().Name + "." + codeGenerator.AppId;
            var plan = new ServicePlan
            {
                DependentServices = contexts
                    .SelectMany(c => c.Value.Services.Select(r => r.Value))
                    .DistinctBy(s => s.URL)
                    .ToArray(),
                Package = new ServicePackage()
            };
            SystemHelper.CreateOrCleanDirectory(name);

            // step: generate composed service code
            var sources = new HashSet<string>();
            var libs = new HashSet<string>();
            var dir = name + ".Source";
            SystemHelper.CreateOrCleanDirectory(dir);

            libs.Add(Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "dsn.dev.csharp.dll"));
            libs.Add(Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "Thrift.dll"));

            var code = codeGenerator.BuildRdsn(serviceObject.GetType(), contexts.Select(c => c.Value).ToArray());
            SystemHelper.StringToFile(code, Path.Combine(dir, name + ".cs"));
            sources.Add(Path.Combine(dir, name + ".cs"));
            
            libs.UnionWith(QueryContext.KnownLibs);

            // step: generate client code for all dependent services
            foreach (var s in contexts
                .SelectMany(c => c.Value.Services.Select(r => r.Value))
                .DistinctBy(s => s.PackageName)
                .Select(s => s.ExtractSpec()))
            {
                var provider = SpecProviderManager.Instance().GetProvider(s.SType);
                Trace.Assert(null != provider, "Language provider missing for type " + s.SType);

                LinkageInfo linkInfo;
                var err = provider.GenerateServiceClient(s, dir, ClientLanguage.Client_CSharp, ClientPlatform.Windows, out linkInfo);
                Trace.Assert(ErrorCode.Success == err);

                sources.UnionWith(linkInfo.Sources);
                libs.UnionWith(linkInfo.DynamicLibraries);
            }

            // step: fill service plan
            plan.Package.Spec = new ServiceSpec
            {
                SType = ServiceSpecType.thrift,
                MainSpecFile = serviceObject.GetType().Name + ".thrift",
                ReferencedSpecFiles = plan.DependentServices
                    .DistinctBy(s => s.PackageName)
                    .Where(s => s.Spec.SType == ServiceSpecType.thrift)
                    .SelectMany(s =>
                    {
                        var spec = s.ExtractSpec();
                        var specFiles = new List<string> { spec.MainSpecFile };

                        SystemHelper.SafeCopy(Path.Combine(spec.Directory, spec.MainSpecFile),
                            Path.Combine(name, spec.MainSpecFile), false);

                        foreach (var ds in spec.ReferencedSpecFiles)
                        {
                            specFiles.Add(ds);
                            SystemHelper.SafeCopy(Path.Combine(spec.Directory, ds), Path.Combine(name, ds), false);
                        }
                        return specFiles;
                    }
                    )
                    .Distinct()
                    .ToList(),
                Directory = name
            };
            plan.Package.MainSpec = ServiceContract.GenerateThriftSpec(serviceObject.GetType(), plan.Package.Spec.ReferencedSpecFiles);
            SystemHelper.StringToFile(plan.Package.MainSpec, Path.Combine(name, plan.Package.Spec.MainSpecFile));

            if (SystemHelper.RunProcess("php.exe", Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "bin", "dsn.generate_code.php") + " " + Path.Combine(name, plan.Package.Spec.MainSpecFile) + " csharp " + dir + " binary layer3") == 0)
            {
                sources.Add(Path.Combine(dir, serviceObject.GetType().Name + ".client.cs"));
                sources.Add(Path.Combine(dir, serviceObject.GetType().Name + ".server.cs"));
                sources.Add(Path.Combine(dir, "ThriftBinaryHelper.cs"));
                sources.Add(Path.Combine(dir, serviceObject.GetType().Name + ".main.composed.cs"));
                sources.Add(Path.Combine(dir, serviceObject.GetType().Name + ".code.definition.cs"));
            }
            else
            {
                Console.Write("php codegen failed");
            }

            //grab thrift-generated files
            sources.UnionWith(Directory.GetFiles(Path.Combine(dir, "thrift"), "*.cs", SearchOption.AllDirectories));

            // step: generate composed service package                        
            CSharpCompiler.ToDiskAssembly(sources.ToArray(), libs.ToArray(), new string[] { },
                Path.Combine(name, name + ".exe"),
                true,
                true
                );

            libs.Add(Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "dsn.core.dll"));
            libs.Add(Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "zookeeper_mt.dll"));
            foreach (var lib in libs.Where(lib => !lib.StartsWith("System.")))
            {
                SystemHelper.SafeCopy(lib, Path.Combine(name, Path.GetFileName(lib)), false);
            }

            //Console.ReadKey();
            return plan;
        }
    }
}
