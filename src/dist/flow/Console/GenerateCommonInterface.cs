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
using System.IO;
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

            var name = Path.GetFileNameWithoutExtension(input);
            return name + ".cs";
            
        }

        public static bool IdlToInterface(string input, string outputDir)
        {
            var toFile = GetToSourceName(input);
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
                case ".proto":
                    translator = null;
                    break;
                case ".thrift":
                    translator = null;
                    break;
                default:
                    break;
                
            }
            return translator.ToCommonInterface(dir, file);              
        }
       
        public override bool Execute(List<string> args)
        {
            if (args.Count < 1)
                return false;

            var input = args[0];
            var output = Path.GetDirectoryName(input);
            IdlToInterface(input, output);

            return true;
        }

        public override string Help()
        {
            return "[g|G]eneratec common interface from IDL files\n"
                + "\tGenerate C# common interface file from IDL files like .thrift, .proto\n"
                ;
        }

        public override string Usage() { return Help(); }

        
    }
}



