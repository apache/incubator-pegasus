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
