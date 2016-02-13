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
using rDSN.Tron.Compiler;
using rDSN.Tron.LanguageProvider;


namespace rDSN.Tron.ControlPanel
{
    public class GenerateIDLFileCommand : Command
    {
        public static string GetToSourceName(string input, string outputType)
        {
            if (!File.Exists(input))
            {
                Console.WriteLine("input file '" + input + "' does not exist");
                return "";
            }

            string name = Path.GetFileNameWithoutExtension(input);
            return name + "." + outputType;
        }

        public static string InterfacesToIdl(string input, string outputDir, string outputType)
        {
            string toFile = GetToSourceName(input, outputType);
            if (toFile == "")
            {
                return "";
            }

            toFile = Path.Combine(outputDir, toFile);

            //
            // code gen 
            //
            CodeBuilder c = new CodeBuilder();
            var asm = Assembly.LoadFrom(input);
            var generator = IdlGenerator.GetInstance(outputType);

            generator.Generate(asm, c);

            StreamWriter writer = new StreamWriter(toFile);
            writer.Write(c.ToString());
            writer.Close();

            return toFile;  
        }

        public override bool Execute(List<string> args)
        {
            if (args.Count < 1)
                return false;

            string input = args[0];
            string outputType = args.ElementAtOrDefault(1);
            var outDir = Path.GetDirectoryName(input);
            string toFile = InterfacesToIdl(input, outDir, outputType);
            if (toFile != "")
            {
                Trace.WriteLine("bond code generated completed, saved in " + toFile);
                return true;
            }
            else
                return false;
        }

        public override string Help()
        {
            return "[g|G]enerateb interfaceAssembly\n"
                + "\tGenerate bond file for the service interfaces defined in the given assembly\n"
                + "\te.g., 'generatei weibo.dll bond' will generate Weibo.bond, with which people can use it for generating various service codes.\n"
                + "\te.g., 'generatei foldername\\weibo.dll proto' will generate Weibo.proto in the same foler.\n"
                ;
        }

        public override string Usage() { return Help(); }
    
    }
    
}
