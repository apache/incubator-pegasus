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

using Microsoft.CSharp;
using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class GenerateCommand : Command
    {
        public static string BondToAssemblyName(string input)
        {
            if (!File.Exists(input))
            {
                Console.WriteLine("input file '" + input + "' does not exist");
                return "";
            }

            if (input.Substring(input.Length - ".bond".Length) != ".bond")
            {
                Console.WriteLine("input file '" + input + "' is not a bond file, not supported for the time being...");
                return "";
            }

            string name = Path.GetFileNameWithoutExtension(input);
            return "rDSN.Tron.App." + name + ".Spec.dll";
        }

        public static string BondToAssembly(string input, string outputDir)
        {
            string binFile = BondToAssemblyName(input);
            if (binFile == "")
            {
                return "";
            }

            binFile = Path.Combine(outputDir, binFile);

            //
            // code gen 
            //
            string tmpDir = Configuration.Instance().Get<string>("TempDir");
            string name = Path.GetFileNameWithoutExtension(input);
            string cmd = @".\resource\bondc.exe /c# /T:.\resource\rules_bond_csharp_csql.txt " + input + " /O:" + tmpDir;
            SystemHelper.RunCommand(cmd);

            //
            // compile
            //
            CompilerParameters cp = new CompilerParameters();

            // Add reference to libs
            cp.ReferencedAssemblies.Add("System.dll");
            cp.ReferencedAssemblies.Add("System.Core.dll");
            cp.ReferencedAssemblies.Add("rDSN.Tron.Utility.dll");
            cp.ReferencedAssemblies.Add("rDSN.Tron.Compiler.dll");
            cp.ReferencedAssemblies.Add("rDSN.Tron.Contract.dll");
            cp.ReferencedAssemblies.Add("Microsoft.Bond.dll");
            cp.ReferencedAssemblies.Add("Microsoft.Bond.Rpc.dll");
            cp.ReferencedAssemblies.Add("Microsoft.Bond.Interfaces.dll");

            // Generate a class library.
            cp.GenerateExecutable = false;

            // Set the assembly file name to generate.
            cp.OutputAssembly = binFile;

            // Save the assembly as a physical file.
            cp.GenerateInMemory = false;

            // optimize the output
            cp.CompilerOptions = "/optimize";
            //cp.CompilerOptions = "/debug";

            // Invoke compilation.
            IDictionary<string, string> providerOptions = new Dictionary<string, string>();
            providerOptions["CompilerVersion"] = "v4.0";
            CSharpCodeProvider provider = new CSharpCodeProvider(providerOptions);

            CompilerResults cr = provider.CompileAssemblyFromFile(cp, new string[] { 
                Path.Combine(tmpDir, name + "_csql.cs"),
                Path.Combine(tmpDir, name + "_types.cs"),
                Path.Combine(tmpDir, name + "_services.cs"),
                Path.Combine(tmpDir, name + "_proxies.cs"),
            });

            if (cr.Errors.Count > 0)
            {
                // Display compilation errors.
                Trace.WriteLine("Compiler error:");
                Trace.WriteLine("Errors building " + cr.PathToAssembly);
                foreach (CompilerError ce in cr.Errors)
                {
                    Trace.WriteLine("\t" + ce.ToString());
                }
                return ""; // compiler error
            }
            else
            {
                return binFile;
            }            
        }

        public override bool Execute(List<string> args)
        {
            if (args.Count < 1)
                return false;

            string input = args[0];
            string binFile = BondToAssembly(input, ".");
            if (binFile != "")
            {
                Trace.WriteLine("code generated completed, saved in " + binFile);
                return true;
            }
            else
                return false;
        }

        public override string Help()
        {
            return "[g|G]enerate bond-file\n"
                + "\tGenerate a dll for the services defined in the spec-file for Tron service composition usage.\n"
                + "\te.g., 'generate weibo.bond' will generate rDSN.Tron.App.weibo.Spec.dll, with which people can use it for service composition and 'weibo' service impl."
                ;
        }

        public override string Usage() { return Help(); }
    }
}
