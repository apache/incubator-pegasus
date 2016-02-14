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
using System.Threading.Tasks;

using System.IO;
using System.Threading;
using System.Diagnostics;


using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.LanguageProvider
{
    public class BondTranslator : IdlTranslator
    {
        public BondTranslator() : base()
        {
            specType = ServiceSpecType.Bond_3_0;
        }

        public BondTranslator(ServiceSpecType t) : base(t) {}
       

        protected  string GetCompilerPath()
        {
            var prefix = "..\\..\\external\\bond\\";
            var path = prefix + "amd64\\bondc.exe";
            if (File.Exists(path))
            {
                return path;
            }
            else
            {
                Console.WriteLine("Cannot find bond compiler at path: {0}!", path);
                return "";
            }


        }

        public override bool ToCommonInterface(string dir, string file, string outDir, List<string> args, bool needCompiled = false)
        {
            var compilerPath = GetCompilerPath();
            if (compilerPath == "")
            {
                return false;
            }
           
            // FIXME: find the template path by compiler path
            var template = "Rules_Bond_CSharp_IDL.tt";
            var arguments = Path.Combine(dir, file)+ " /c#" + " /T:" + template + " /O:" + outDir;

            if (SystemHelper.RunProcess(compilerPath, arguments) == 0)
            {
                var csFile = Path.GetFileNameWithoutExtension(file) + extension;
                var output = Path.Combine(outDir, csFile);
                return Compile(output);
            }
            else
            {
                return false;
            }

            
        }

      

    }
}
