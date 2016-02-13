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

namespace rDSN.Tron.ControlPanel
{
    public class GenerateCommonInterfaceCommand : Command
    {
        public static string GetToSourceName(string input)
        {
            if (!File.Exists(input))
            {
                Console.WriteLine("input file '" + input + "' does not exist");
                return "";
            }

            string name = Path.GetFileNameWithoutExtension(input);
            return name + ".cs";
            
        }

        public static bool IdlToInterface(string input, string outputDir)
        {
            string toFile = GetToSourceName(input);
            if (toFile == "")
            {
                return false;
            }

            toFile = Path.Combine(outputDir, toFile);
            var extension = Path.GetExtension(input);
            var dir = Path.GetDirectoryName(input);
            var file = Path.GetFileName(input);
            IdlTranslator translator = null;
            
            // TODO: using class factory to generate the instance
            switch (extension) 
            {
                case ".bond":
                    translator = new BondTranslator();
                    break;
                case ".proto":
                    translator = null;
                    break;
                case ".thrift":
                    translator = null;
                    break;
                default:
                    break;
                
            }
            if (translator.ToCommonInterface(dir, file)) 
            {
                return true;
            }
            else
            {
                return false;
            }


            

            /*
            Grammar grammar = new BondGrammar();
            var language = new LanguageData(grammar);
            var parser = new Parser(language);
            var content = File.ReadAllText(input);
            var parseTree = parser.Parse(content);
            Trace.Assert(!parseTree.HasErrors());



            //
            // code gen 
            //
            CodeBuilder c = new CodeBuilder();
            c.AppendLine("using System;");
            c.AppendLine("using System.Collections.Generic;");
            c.AppendLine("using System.Linq;");
            c.AppendLine("using System.Text;");
            c.AppendLine("using System.Diagnostics;");
            c.AppendLine();


            generateEnums(parseTree, c);
            generateStructs(parseTree, c);
            generateServices(parseTree, c);
          
            StreamWriter writer = new StreamWriter(toFile);
            writer.Write(c.ToString());
            writer.Close();
             */

              
        }
       
        public override bool Execute(List<string> args)
        {
            if (args.Count < 1)
                return false;

            string input = args[0];
            string output = Path.GetDirectoryName(input);
            try
            {
                IdlToInterface(input, output);
            }
            catch (Exception)
            {
                throw;
            }
            
            return true;
        }

        public override string Help()
        {
            return "[g|G]eneratec common interface from IDL files\n"
                + "\tGenerate C# common interface file from IDL files like .bond, .thrift, .proto\n"
                + "\te.g., 'generatec weibo.bond' will generate Weibo.cs.\n"
                ;
        }

        public override string Usage() { return Help(); }

        
    }
}



