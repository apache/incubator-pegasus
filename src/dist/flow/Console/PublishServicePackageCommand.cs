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
using System.IO;
using System.Xml.Serialization;
using System.CodeDom.Compiler;
using System.Reflection;
using System.Collections;
using System.Linq.Expressions;
using System.Diagnostics;
using System.IO.Compression;
using System.Windows.Forms;
using System.Threading;

using BondNetlibTransport;
using BondTransport;

using Microsoft.CSharp;
using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;
using rDSN.Tron.Contract;
using rDSN.Tron.LanguageProvider;

namespace rDSN.Tron.ControlPanel
{
    public class PublishServicePackageCommand : Command
    {
        public PublishServicePackageCommand()
        {
            _transport = new NetlibTransport(new BinaryProtocolFactory());
            InitProxy();
        }

        private void InitProxy()
        {
            NodeAddress storeAddress = new NodeAddress();
            storeAddress.Host = Configuration.Instance().Get<string>("ServiceStore", "Host", "localhost");
            storeAddress.Port = Configuration.Instance().Get<int>("ServiceStore", "Port", 19995);

            _proxy = new ServiceStore_Proxy(_transport.Connect(storeAddress.Host, storeAddress.Port));
        }

        public static bool FillServiceImpl(ServicePackage cls, string dir, string configFile)
        {
            Configuration config = new Configuration(Path.Combine(dir, configFile));

            if ("" == (cls.Name = config.Get<string>("Service", "Name", "")))
            {
                Console.WriteLine("[Service] Name undefined");
                return false;
            }
                        
            if ("" == (cls.MainExecutableName = config.Get<string>("Service", "MainExecutableName", "")))
            {
                Console.WriteLine("[Service] MainExecutableName undefined");
                return false;
            }

            if (!File.Exists(Path.Combine(dir, cls.MainExecutableName)))
            {
                Console.WriteLine("MainExecutableName '" + cls.MainExecutableName + "' not exist in '" + dir + "'");
                return false;
            }
                        
            cls.Arguments = config.Get<string>("Service", "Arguments", "");
            if (!cls.Arguments.Contains("%port%"))
            {
                Console.WriteLine("[Service] Arguments must contain '%port%', which will be replaced as a RPC port designated by cluster scheduler");
                return false;
            }

            if ("" == (cls.Author = config.Get<string>("Service", "Author", "")))
            {
                Console.WriteLine("[Service] Author undefined");
                return false;
            }

            if ("" == (cls.Description = config.Get<string>("Service", "Description", "")))
            {
                Console.WriteLine("[Service] Description undefined");
                return false;
            }

            if ("" == (cls.IconFileName = config.Get<string>("Service", "IconFileName", "")))
            {
                Console.WriteLine("[Service] IconFileName undefined");
                return false;
            }

            cls.Spec.Directory = dir;
            if (ServiceSpecType.Unknown == (cls.Spec.SType = config.Get<ServiceSpecType>("Service", "ServiceSpecType", ServiceSpecType.Unknown)))
            {
                Console.WriteLine("[Service] ServiceSpecType undefined");
                return false;
            }
            Trace.Assert(cls.Spec.SType == ServiceSpecType.Bond_3_0 || cls.Spec.SType == ServiceSpecType.Thrift_0_9, "only BOND_3_0 is supported now");

            if ("" == (cls.Spec.MainSpecFile = config.Get<string>("Service", "MainSpecFile", "")))
            {
                Console.WriteLine("[Service] MainSpecFile undefined");
                return false;
            }

            if (!File.Exists(Path.Combine(dir, cls.Spec.MainSpecFile)))
            {
                Console.WriteLine("MainSpecFile '" + cls.Spec.MainSpecFile + "' not exist in '" + dir + "'");
                return false;
            }

            cls.MainSpec = SystemHelper.FileToString(Path.Combine(dir, cls.Spec.MainSpecFile));

            var specs = config.GetSection("ReferencedSpecFiles");
            if (null != specs)
            {
                foreach (var kv in specs)
                {
                    cls.Spec.ReferencedSpecFiles.Add(kv.Key);
                }    
            }

            var provider = SpecProviderManager.Instance().GetProvider(cls.Spec.SType);
            Trace.Assert(provider != null, "language provider is missing for " + cls.Spec.SType.ToString());
            
            // get csharp spec source
            var sources = provider.ToCommonSpec(cls.Spec, ".");

            // compile to assembly
            CSharpCompiler.ToDiskAssembly(sources, new string[] { "rDSN.Tron.Utility.dll", "rDSN.Tron.Contract.dll" }, new string[]{}, 
                Path.Combine(dir, cls.Name + ".Tron.Spec.dll")
                ); 

            // code generation for composition stubs
            CodeBuilder c = new CodeBuilder();
            c.AppendLine("using System;");
            c.AppendLine("using System.Collections.Generic;");
            c.AppendLine("using System.Linq;");
            c.AppendLine("using System.Text;");
            c.AppendLine("using System.Diagnostics;");
            c.AppendLine();
            c.AppendLine("using rDSN.Tron.Utility;");
            c.AppendLine("using rDSN.Tron.Contract;");
            //c.AppendLine("using rDSN.Tron.Compiler;");
            c.AppendLine();

            // service composition stubs
            var asm = Assembly.LoadFrom(Path.Combine(dir, cls.Name + ".Tron.Spec.dll"));
            foreach (var s in asm.GetExportedTypes().Where(t => ServiceContract.IsTronService(t)))
            {
                c.AppendLine("namespace " + s.Namespace);
                c.BeginBlock();

                ServiceContract.GenerateCompositionStub(s, cls, c);

                c.EndBlock();
                c.AppendLine();
            }

            StreamWriter writer = new StreamWriter(Path.Combine(dir, cls.Name + ".Tron.Composition.cs"));
            writer.Write(c.ToString());
            writer.Close();
            
            // compile to composition assembly
            string casmbly = Path.Combine(dir, cls.Name + ".Tron.Composition.dll");
            List<string> sources2 = new List<string>();
            foreach (var s in sources)
                sources2.Add(s);
            sources2.Add(Path.Combine(dir, cls.Name + ".Tron.Composition.cs"));

            // embed all spec files
            List<string> resources = new List<string>();
            resources.Add(Path.Combine(dir, cls.Spec.MainSpecFile));
            foreach (var sc in cls.Spec.ReferencedSpecFiles)
                resources.Add(Path.Combine(dir, sc));

            CSharpCompiler.ToDiskAssembly(sources2.ToArray(),
                new string[] { "rDSN.Tron.Utility.dll", "rDSN.Tron.Contract.dll", "Microsoft.Bond.Interfaces.dll" }, 
                resources.ToArray(),
                casmbly, false, true
                );
                        
            // fill composition assembly field
            cls.CompositionAssemblyContent = new Microsoft.Bond.BondBlob(SystemHelper.FileToByteArray(casmbly));
            return true;
        }

        public static ServiceAPI LoadAPIFromInterface(Type ifs)
        {
            if (!ServiceContract.IsTronService(ifs))
            {
                throw new Exception("the given type '" + ifs.FullName + "' is not annotated as 'TronService'.");
            }

            ServiceAPI api = new ServiceAPI();

            api.Name = ifs.FullName;
            
            // fill all methods
            foreach (var m in ServiceContract.GetServiceCalls(ifs))
            {
                if (m.IsConstructor)
                    continue;

                ServiceMethod sm = new ServiceMethod();
                sm.Name = m.Name;

                Trace.Assert(m.GetParameters().Length == 1);

                sm.InputTypeFullName = m.GetParameters()[0].ParameterType.FullName;
                sm.OutputTypeFullName = m.ReturnType == typeof(void) ? "void" : m.ReturnType.FullName;
                sm.ParameterName = m.GetParameters()[0].Name;
                
                api.Methods.Add(m.Name, sm);
            }

            return api;
        }

        public static ServicePackage LoadServiceClass(string folderName)
        {
            ServicePackage cls = new ServicePackage();

            Console.WriteLine("!!! All resources necessary for starting the service instance must be first put into one directory.");

            if (!Directory.Exists(folderName))
            {
                Console.WriteLine("folder '" + folderName + "' does not exist");
                return null;
            }
            
            string iniFilePath = Path.Combine(folderName, "servicedef.ini");
            bool fillOk = false;
            if (File.Exists(iniFilePath))
            {
                fillOk = FillServiceImpl(cls, folderName, "servicedef.ini");
            }

            if (!fillOk)
            {
                Console.WriteLine("Please fill your " + iniFilePath + " appropriately before publish the service (exampled below)");
                Console.WriteLine("Please also ensure all resources file needed to start the service are put into this directory");
                Console.WriteLine();
                Console.WriteLine("[Service]");
                Console.WriteLine("Name = MySQL");
                Console.WriteLine("MainExecutableName = rDSN.Tron.App.ServiceHost.exe");
                Console.WriteLine("Arguments = rDSN.Tron.App.MySQL.dll %port% rDSN.Tron.App.MySQL.MySqlClient_Service_Impl3");
                Console.WriteLine("ServiceSpecType = Bond_3_0");
                Console.WriteLine("MainSpecFile = mysql.bond");
                Console.WriteLine("[ReferencedSpecFiles]");
                Console.WriteLine("// put included .bond files here in %MainSpecFile%");
                Console.WriteLine();
                return null;
            }

            try
            {
                File.Delete("tmp.zip");
            }
            catch (Exception)
            { }

            ZipFile.CreateFromDirectory(folderName, "tmp.zip");
            cls.PackageZip = new Microsoft.Bond.BondBlob(SystemHelper.FileToByteArray("tmp.zip"));
                        
            return cls;
        }
        
        public override bool Execute(List<string> args)
        {
            if (args.Count < 1)
            {
                return false;
            }

            var svc = LoadServiceClass(args[0]);
            if (null == svc)
                return false;

            Console.WriteLine("service loaded as:");
            svc.Echo(svc.Name, 1, 2);

            for (int ik = 0; ik < 2; ik++)
            {
                try
                {
                    var resp = _proxy.RegisterServicePackage(svc);

                    ErrorCode c = (ErrorCode)resp.Code;
                    Console.WriteLine("service implementation registration error code: " + c.ToString());
                    return true;
                }
                catch (Exception e)
                {
                    Console.WriteLine("exception, msg = " + e.Message);
                    InitProxy();
                }
            }
            return false;
        }

        public override string Help()
        {
            return "PublishPackage|PP|pp dir\n"
                + "\tPublish a service implementation package in the given dir.\n"
                ;
        }

        public override string Usage() { return Help(); }

        private ITransport _transport;
        private ServiceStore_Proxy _proxy;
    }
}
