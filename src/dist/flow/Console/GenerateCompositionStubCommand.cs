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

using Microsoft.CSharp;
using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;
using rDSN.Tron.Contract;
using rDSN.Tron.LanguageProvider;

namespace rDSN.Tron.ControlPanel
{
    public class GenerateCompositionStubCommand : Command
    {

        public override bool Execute(List<string> args)
        {
            Console.WriteLine(args);
            bool is_rdsn_rpc;
            ServiceSpecType idl_type;
            if (args[0] == "thrift")
            {
                idl_type = ServiceSpecType.Thrift_0_9; 
            }
            else if (args[0] == "protobuf")
            {
                idl_type = ServiceSpecType.Proto_Buffer_1_0;
            }
            else
            {
                throw new NotImplementedException();
            }
            if (args[1] == "dsn")
            {
                is_rdsn_rpc = true;
            }
            else
            {
                is_rdsn_rpc = false;
            }
            var app_name = Path.GetFileNameWithoutExtension(args[2]);
            
            if (SystemHelper.RunProcess("php.exe", Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "bin/dsn.generate_code.php") + " " + args[2] + " csharp tmp binary layer3") != 0)
            {
                return false;
            }
            CSharpCompiler.ToDiskAssembly(
                new string[] { Path.Combine( "tmp" , app_name + ".commonspec.cs" )  },
                new string[] { "rDSN.Tron.Utility.dll", "rDSN.Tron.Contract.dll" },
                new string[] { },
                Path.Combine("tmp", app_name + ".Tron.Spec.dll")
            );

            var c = new CodeBuilder();
            c.AppendLine("using System;");
            c.AppendLine("using System.Collections.Generic;");
            c.AppendLine("using System.Linq;");
            c.AppendLine("using System.Text;");
            c.AppendLine("using System.Diagnostics;");
            c.AppendLine();
            c.AppendLine("using rDSN.Tron.Utility;");
            c.AppendLine("using rDSN.Tron.Contract;");
            c.AppendLine("using rDSN.Tron;");
            //c.AppendLine("using rDSN.Tron.Compiler;");
            c.AppendLine();

            // service composition stubs
            var asm = Assembly.LoadFrom(Path.Combine("tmp", app_name + ".Tron.Spec.dll"));
                
            var cls = new ServicePackage();
            cls.Spec.MainSpecFile = cls.MainSpec = Path.GetFileName(args[2]);
            cls.Spec.SType = idl_type;
            cls.Spec.IsRdsnRpc = is_rdsn_rpc;

            foreach (var s in asm.GetExportedTypes().Where(t => ServiceContract.IsTronService(t)))
            {
                c.AppendLine("namespace " + s.Namespace);
                c.BeginBlock();

                ServiceContract.GenerateCompositionStub(s, cls, c);

                c.EndBlock();
                c.AppendLine();
            }

            StreamWriter writer = new StreamWriter(Path.Combine("tmp", app_name + ".Tron.Composition.cs"));
            writer.Write(c.ToString());
            writer.Close();

            CSharpCompiler.ToDiskAssembly(new string[] { Path.Combine("tmp", app_name + ".commonspec.cs"),  Path.Combine("tmp", app_name + ".Tron.Composition.cs") },
                new string[] { "rDSN.Tron.Utility.dll", "rDSN.Tron.Contract.dll" },
                new string[] { args[2] },
                Path.Combine("tmp", app_name + ".Tron.Composition.dll"),
                false, true
                );

            return true;
        }

        public override string Help()
        {
            return ".\\Tron.exe gcs thrift dsn .\\add.thrift";
        }
    }
}
