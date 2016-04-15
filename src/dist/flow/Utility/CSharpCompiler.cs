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
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.CSharp;

namespace rDSN.Tron.Utility
{
    public class CSharpCompiler
    {
        public static Assembly ToMemoryAssembly(string[] sources, string[] refAssemblies, string[] resourceFiles,
            bool executable = false,
            bool debug = false,
            string version = "v4.0"
            )
        {
            return ToAssembly(sources, refAssemblies, resourceFiles, "", executable, debug, version, true);
        }

        public static bool ToDiskAssembly(string[] sources, string[] refAssemblies, string[] resourceFiles, string outputAssembly, 
            bool executable = false,
            bool debug = false,
            string version = "v4.0"
            )
        {
            return null != ToAssembly(sources, refAssemblies, resourceFiles, outputAssembly, executable, debug, version, false);
        }

        private static Assembly ToAssembly(string[] sources, string[] refAssemblies, string[] resourceFiles, string outputAssembly, 
            bool executable = false,             
            bool debug = false,
            string version = "v4.0",
            bool inMemory = false
            )
        {
            var cp = new CompilerParameters();

            // Add reference to libs
            cp.ReferencedAssemblies.Add("System.dll");
            cp.ReferencedAssemblies.Add("System.Core.dll");

            foreach (var s in refAssemblies)
            {
                cp.ReferencedAssemblies.Add(s);
            }

            // Generate a class library.
            cp.GenerateExecutable = executable;

            // Set the assembly file name to generate.
            cp.OutputAssembly = outputAssembly;

            // Save the assembly as a physical file.
            cp.GenerateInMemory = inMemory;

            // optimize the output
            cp.CompilerOptions = (debug ? "/debug" : "/optimize");

            // embed resourceFiles
            foreach (var r in resourceFiles)
            {
                cp.EmbeddedResources.Add(r);
            }

            // Invoke compilation.
            IDictionary<string, string> providerOptions = new Dictionary<string, string>();
            providerOptions["CompilerVersion"] = version;
            var provider = new CSharpCodeProvider(providerOptions);


            var cr = provider.CompileAssemblyFromFile(cp, sources);
            if (cr.Errors.Count > 0)
            {
                // Display compilation errors.
                Console.WriteLine("Compiler error:");
                Console.WriteLine("Errors building " + cr.PathToAssembly);
                foreach (CompilerError ce in cr.Errors)
                {
                    Console.WriteLine("\t" + ce);
                }

                return null;
            }
            var oasm = cr.CompiledAssembly;

            foreach (var stream in resourceFiles.Select(Path.GetFileName).Select(oasm.GetManifestResourceStream))
            {
                Trace.Assert(stream != null);
            }

            Console.WriteLine("Generate " + cr.PathToAssembly + " succeeds!");
            return cr.CompiledAssembly;
        }
    }
}
