using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml.Serialization;
using System.CodeDom.Compiler;
using System.Reflection;
using System.Collections;
using System.Linq.Expressions;
using System.Diagnostics;

using Microsoft.CSharp;
using rDSN.Tron.Utility;
using rDSN.Tron.Contract;
using rDSN.Tron.LanguageProvider;

namespace rDSN.Tron.Compiler
{
    public class Compiler
    {
        public static ServicePlan Compile(Type serviceType)
        {
            var calls = new Dictionary<MethodInfo, MethodCallExpression>();
            var ctor = serviceType.GetConstructor(new Type[] { });
            var this_ = ctor.Invoke(new object[] { });

            foreach (var m in ServiceContract.GetServiceCalls(serviceType))
            {
                var req = m.GetParameters()[0].ParameterType.GetConstructor(new Type[] { typeof(string) }).Invoke(new object[] { "request" });
                var result = (ISymbol)m.Invoke(this_, new object[] { req });

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

            Dictionary<MethodInfo, QueryContext> contexts = new Dictionary<MethodInfo, QueryContext>();

            while (workerQueue.Count > 0)
            {
                var exp = workerQueue.Dequeue();
                QueryContext context = new QueryContext(serviceObject, exp.Value, exp.Key.Name);
                context.Collect();

                contexts.Add(exp.Key, context);

                foreach (var s in context.ExternalComposedSerivce)
                {
                    if (!contexts.ContainsKey(s.Key))
                        workerQueue.Enqueue(new KeyValuePair<MethodInfo, MethodCallExpression>(s.Key, s.Value));
                }
            }

            // step: prepare service plan
            CodeGenerator codeGenerator = new CodeGenerator();  
            string name = serviceObject.GetType().Name + "." + codeGenerator.AppId.ToString();
            ServicePlan plan = new ServicePlan();
            plan.DependentServices = contexts
                .SelectMany(c => c.Value.Services.Select(r => r.Value))
                .DistinctBy(s => s.URL)
                .ToArray()
                ;
            plan.Package = new ServicePackage();
            SystemHelper.CreateOrCleanDirectory(name);

            // step: generate composed service code
            HashSet<string> sources = new HashSet<string>();
            HashSet<string> libs = new HashSet<string>();                      
            string dir = name + ".Source";
            SystemHelper.CreateOrCleanDirectory(dir);

            string code = codeGenerator.Build(serviceObject.GetType().Name, contexts.Select(c => c.Value).ToArray());            
            SystemHelper.StringToFile(code, Path.Combine(dir, name + ".cs"));                        
            sources.Add(Path.Combine(dir, name + ".cs"));
            SystemHelper.StringToFile(ServiceContract.GenerateComposedServiceImplementation(serviceObject.GetType()),
                Path.Combine(dir, name + "_ServiceWrapper.cs"));
            sources.Add(Path.Combine(dir, name + "_ServiceWrapper.cs"));

            //foreach (var lib in contexts.SelectMany(q => q.Value.AllLibraries))
            //{
            //    if (!libs.Contains(lib.Key))
            //        libs.Add(lib.Key);
            //}
            foreach (var lib in QueryContext.KnownLibs)
            {
                if (!libs.Contains(lib))
                    libs.Add(lib);
            }

            // step: generate client code for all dependent services
            foreach (var s in contexts
                .SelectMany(c => c.Value.Services.Select(r => r.Value))
                .DistinctBy(s => s.PackageName)
                .Select(s => s.ExtractSpec()))
            {
                var provider = SpecProviderManager.Instance().GetProvider(s.SType);
                Trace.Assert(null != provider, "Language provider missing for type " + s.SType.ToString());

                LinkageInfo linkInfo = new LinkageInfo();
                var err = provider.GenerateServiceClient(s, dir, ClientLanguage.Client_CSharp, ClientPlatform.Windows, out linkInfo);
                Trace.Assert(ErrorCode.Success == err);

                foreach (var source in linkInfo.Sources)
                {
                    sources.Add(Path.Combine(dir, source));
                }

                foreach (var lib in linkInfo.DynamicLibraries)
                {
                    if (!libs.Contains(lib))
                        libs.Add(lib);
                }
            }

            // step: fill service plan
            plan.Package.Spec = new ServiceSpec();
            plan.Package.Spec.SType = ServiceSpecType.Bond_3_0;
            plan.Package.Spec.MainSpecFile = name + ".bond";
            plan.Package.Spec.ReferencedSpecFiles = plan.DependentServices
                .DistinctBy(s => s.PackageName)
                .SelectMany(s =>
                {
                    var spec = s.ExtractSpec();
                    List<string> specFiles = new List<string>();
                    
                    specFiles.Add(spec.MainSpecFile);
                    SystemHelper.SafeCopy(Path.Combine(spec.Directory, spec.MainSpecFile), Path.Combine(name, spec.MainSpecFile), false);

                    foreach (var ds in spec.ReferencedSpecFiles)
                    {
                        specFiles.Add(ds);
                        SystemHelper.SafeCopy(Path.Combine(spec.Directory, ds), Path.Combine(name, ds), false);
                    }
                    return specFiles;
                }
                )
                .Distinct()
                .ToList();
            plan.Package.Spec.Directory = name;
            plan.Package.MainSpec = ServiceContract.GenerateBondSpec(serviceObject.GetType(), plan.Package.Spec.ReferencedSpecFiles);            
            SystemHelper.StringToFile(plan.Package.MainSpec, Path.Combine(name, plan.Package.Spec.MainSpecFile));
            plan.Package.MainExecutableName = "rDSN.Tron.App.ServiceHost.exe";
            plan.Package.Arguments = "%name% " + name + ".dll %port%";
            plan.Package.Name = name;
            SystemHelper.SafeCopy("rDSN.Tron.App.ServiceHost.exe", Path.Combine(name, "rDSN.Tron.App.ServiceHost.exe"), true);
            plan.Package.Author = "XYZ";
            plan.Package.Description = "Auto generated composed service";
            plan.Package.IconFileName = "abc.png";

            // step: generate service sketch files
            var provider2 = SpecProviderManager.Instance().GetProvider(plan.Package.Spec.SType);
            Trace.Assert(null != provider2, "Language provider missing for type " + plan.Package.Spec.SType.ToString());
            LinkageInfo linkInfo2 = new LinkageInfo();
            var err2 = provider2.GenerateServiceSketch(plan.Package.Spec, dir, ClientLanguage.Client_CSharp, ClientPlatform.Windows, out linkInfo2);
            Trace.Assert(ErrorCode.Success == err2);

            foreach (var source in linkInfo2.Sources)
            {
                sources.Add(Path.Combine(dir, source));
            }
            foreach (var lib in linkInfo2.DynamicLibraries)
            {
                if (!libs.Contains(lib))
                    libs.Add(lib);
            }

            // step: fill servicedef.ini file
            string def = "[Service]\r\n"
                        + "Name = " + serviceObject.GetType().Name  + "\r\n"
                        + "MainExecutableName = " + plan.Package.MainExecutableName + "\r\n"
                        + "Arguments = " + plan.Package.Arguments +"\r\n"
                        + "ServiceSpecType = " + plan.Package.Spec.SType +  " \r\n"
                        + "MainSpecFile = " + plan.Package.Spec.MainSpecFile + "\r\n"
                        + "Author = " + plan.Package.Author + "\r\n"
                        + "Description = " + plan.Package.Description + "\r\n"
                        + "IconFileName = " + plan.Package.IconFileName + "\r\n"
                        + "\r\n"
                        + "[ReferencedSpecFiles]\r\n"
                        + plan.Package.Spec.ReferencedSpecFiles.VerboseCombine("\r\n", s => s);

            SystemHelper.StringToFile(def, Path.Combine(name, "servicedef.ini"));

            // step: generate composed service package                        
            CSharpCompiler.ToDiskAssembly(sources.ToArray(), libs.ToArray(), new string[] { },
                Path.Combine(name, name + ".dll"),

                false,
                true
                );
            foreach (var lib in libs)
            {
                // TODO: fix me as this is a hack
                if (!lib.StartsWith("System."))
                {
                    SystemHelper.SafeCopy(lib, Path.Combine(name, Path.GetFileName(lib)), false);
                }
            }
            
            return plan;

            //// step: identify all calls to services
            //LGraph g = new LGraph();

            //PrimitiveGraphBuilder builder = new PrimitiveGraphBuilder(serviceObject, expression, g);
            //builder.Build();
            //g.VisualizeGraph("c:\\abc", "pass1");

            //BasicBlockBuilder builder2 = new BasicBlockBuilder(g);
            //builder2.Build();

            //g.Vertices.Select(v => { v.Value.DumpInstructions(); return 0; }).Count();

            //g.VisualizeGraph("c:\\abc", "pass2");

            ////builder.Report();

            //return "";
        }
    }
}
