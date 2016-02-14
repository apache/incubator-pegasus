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
using System.IO.Compression;
using System.Reflection;

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;
using rDSN.Tron.LanguageProvider;

namespace rDSN.Tron.ControlPanel
{
    public class GetServiceClientLibCommand : Command
    {
        public GetServiceClientLibCommand()
        {
            _transport = new NetlibTransport(new BinaryProtocolFactory());
            _saveDir = Configuration.Instance().Get<string>("ServicePackageSaveDir", Path.Combine(Directory.GetCurrentDirectory(), "ServicePackage"));
            if (!Directory.Exists(_saveDir))
            {
                Directory.CreateDirectory(_saveDir);
            }

            InitProxy();
        }

        public override bool Execute(List<string> args)
        {
            var req = new Name();
            if (args.Count == 0)
            {
                Console.WriteLine(Usage());
                return false;
            }

            try
            {
                req.Value = args[0];
                var resp = _proxy.GetServiceCompositionAssembly(req);

                ErrorCode c = (ErrorCode)resp.Code;
                Console.WriteLine("query error code: " + c.ToString());

                if (c == ErrorCode.Success)
                {
                    var sc = resp.Value;

                    SystemHelper.ByteArrayToFile(
                        sc.Data.Array,
                        sc.Data.Offset,
                        sc.Data.Count,
                        Path.Combine(_saveDir, args[0] + ".Tron.Composition.dll")
                        );

                    Assembly asm = Assembly.LoadFrom(Path.Combine(_saveDir, args[0] + ".Tron.Composition.dll"));
                    var ss = asm.ExportedTypes.Where(t => t.IsInheritedTypeOf(typeof(rDSN.Tron.Contract.Service))).ToArray();
                    if (ss.Length == 0)
                    {
                        Console.WriteLine("The package does not contain any service, abort the task");
                        return false;
                    }

                    var svc = (rDSN.Tron.Contract.Service)ss[0].GetConstructor(new Type[] { typeof(string), typeof(string) }).Invoke(new object[] { "test", "test" });
                    var spec = svc.ExtractSpec();
                    var provider = SpecProviderManager.Instance().GetProvider(spec.SType);
                    var linkInfo = new LinkageInfo();
                    var err = provider.GenerateServiceClient(spec, spec.Directory, ClientLanguage.Client_CSharp, ClientPlatform.Windows, out linkInfo);
                    if (err != ErrorCode.Success)
                    {
                        Console.WriteLine("Service client code generation failed, err = " + err);
                        return false;
                    }

                    if (true == CSharpCompiler.ToDiskAssembly(
                        linkInfo.Sources.Select(s => Path.Combine(spec.Directory, s)).ToArray(),
                        linkInfo.DynamicLibraries.ToArray(),
                        new string[] { },
                        Path.Combine(spec.Directory, args[0] + ".Client.dll")
                        ))
                    {
                        Console.WriteLine("Clientlib for service '" + args[0] + "' is downloaded to " + Path.Combine(spec.Directory, args[0] + ".Client.dll"));
                        return true;
                    }
                    else
                    {
                        Console.WriteLine("Client code generated in '" + spec.Directory + "', but compilation failed");
                        return false;
                    }
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                Console.WriteLine("exception, msg = " + e.Message);
                InitProxy();
            }

            return false;
        }

        private void InitProxy()
        {
            NodeAddress storeAddress = new NodeAddress();
            storeAddress.Host = Configuration.Instance().Get<string>("ServiceStore", "Host", "localhost");
            storeAddress.Port = Configuration.Instance().Get<int>("ServiceStore", "Port", 19995);

            _proxy = new ServiceStore_Proxy(_transport.Connect(storeAddress.Host, storeAddress.Port));
        }

        public override string Help()
        {
            return "GetServiceClient|GSC|gsc ServicePackageName";
        }

        public override string Usage()
        {
            return Help();
        }

        private ITransport _transport;
        private ServiceStore_Proxy _proxy;
        private string _saveDir;
    }
}
