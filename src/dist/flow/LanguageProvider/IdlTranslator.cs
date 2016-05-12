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
using System.Runtime.InteropServices;
using rDSN.Tron.Utility;

namespace rDSN.Tron.LanguageProvider
{
    public abstract class IdlTranslator
    {
        public IdlTranslator()
        {

        }
        public IdlTranslator(ServiceSpecType t)
        {
            specType = t;
        }

        public ServiceSpecType GetSpecType()
        {
            return specType;
        }

        public static IdlTranslator GetInstance(ServiceSpecType t)
        {
            switch(t)
            {
                case ServiceSpecType.Thrift:
                    return new ThriftTranslator();
                case ServiceSpecType.Proto:
                    throw new NotImplementedException();
                default:
                    return null;
            }
        }

        public virtual bool ToCommonInterface(string dir, string file)
        {
            return ToCommonInterface(dir, file, dir, null, false);
        }
        public virtual bool ToCommonInterface(string dir, string file, List<string> args)
        {
            return ToCommonInterface(dir, file, dir, args, false);
        }
        public virtual bool ToCommonInterface(string dir, string file, string outDir) 
        {
            return ToCommonInterface(dir, file, outDir, null, false);
        }

        /// <summary>
        /// translate the target IDL file to Common Interface
        /// </summary>
        /// <param name="dir"></param>
        /// <param name="file"></param>
        /// <param name="outDir"></param>
        /// /// <param name="args"></param>
        /// <returns></returns>
        public abstract bool ToCommonInterface(string dir, string file, string outDir, List<string> args, bool needCompiled = false);


        
        protected bool Compile(string input)
        {
            return Compile(input, Path.GetDirectoryName(input));
        }

        protected static bool Compile(string input, string outDir)
        {
            var dir = Path.GetDirectoryName(input);
            var file = Path.GetFileName(input);

            var output = Path.Combine(outDir, Path.GetFileNameWithoutExtension(file) + ".dll");
            var cscPath = Path.Combine(RuntimeEnvironment.GetRuntimeDirectory(), "csc.exe");
            const string libFiles = "rDSN.Tron.Contract.dll ";
            var arguments = "/target:library /out:" + output + " " + input + " /r:" + libFiles;

            // if input is a folder, the default action is to compile all *.cs files in the folder recursively
            if (Directory.Exists(input))
            {
                return false;
            }

            if (!Directory.Exists(dir))
            {
                Console.WriteLine(dir + "does not exist!");
                return false;
            }
            if (!File.Exists(input))
            {
                Console.WriteLine(file + " does not exist!");
                return false;
            }
            
            return SystemHelper.RunProcess(cscPath, arguments) == 0;

        }

        protected ServiceSpecType specType;
        protected const string extension = ".cs";

    }
}
