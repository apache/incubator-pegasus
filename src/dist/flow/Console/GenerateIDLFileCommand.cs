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
using System.IO;
using System.Reflection;
using System.Diagnostics;
using rDSN.Tron.Utility;
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

            var name = Path.GetFileNameWithoutExtension(input);
            return name + "." + outputType;
        }

        public static string InterfacesToIdl(string input, string outputDir, string outputType)
        {
            var toFile = GetToSourceName(input, outputType);
            if (toFile == "")
            {
                return "";
            }

            toFile = Path.Combine(outputDir, toFile);

            //
            // code gen 
            //
            var c = new CodeBuilder();
            var asm = Assembly.LoadFrom(input);
            var generator = IdlGenerator.GetInstance(outputType);

            generator.Generate(asm, c);

            var writer = new StreamWriter(toFile);
            writer.Write(c.ToString());
            writer.Close();

            return toFile;  
        }

        public override bool Execute(List<string> args)
        {
            if (args.Count < 1)
                return false;

            var input = args[0];
            var outputType = args.ElementAtOrDefault(1);
            var outDir = Path.GetDirectoryName(input);
            var toFile = InterfacesToIdl(input, outDir, outputType);
            if (toFile != "")
            {
                Trace.WriteLine("idl code generated completed, saved in " + toFile);
                return true;
            }
            else
                return false;
        }

        public override string Help()
        {
            return "[g|G]enerateb interfaceAssembly\n"
                + "\tGenerate idl file for the service interfaces defined in the given assembly\n"
                + "\te.g., 'generatei weibo.dll thrift' will generate Weibo.thrift, with which people can use it for generating various service codes.\n"
                + "\te.g., 'generatei foldername\\weibo.dll proto' will generate Weibo.proto in the same foler.\n"
                ;
        }

        public override string Usage() { return Help(); }
    
    }
    
}
