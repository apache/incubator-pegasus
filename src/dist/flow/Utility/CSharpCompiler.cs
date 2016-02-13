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
            CompilerParameters cp = new CompilerParameters();

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
            CSharpCodeProvider provider = new CSharpCodeProvider(providerOptions);


            CompilerResults cr = provider.CompileAssemblyFromFile(cp, sources);
            if (cr.Errors.Count > 0)
            {
                // Display compilation errors.
                Trace.WriteLine("Compiler error:");
                Trace.WriteLine("Errors building " + cr.PathToAssembly);
                foreach (CompilerError ce in cr.Errors)
                {
                    Trace.WriteLine("\t" + ce.ToString());
                }

                return null;
            }
            else
            {
                var oasm = cr.CompiledAssembly;

                foreach (var r in resourceFiles)
                {
                    var name = Path.GetFileName(r);
                    var stream = oasm.GetManifestResourceStream(name);
                    Trace.Assert(stream != null);
                }

                return cr.CompiledAssembly;
            }
        }
    }
}
