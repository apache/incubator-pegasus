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
using System.IO.Compression;
using System.Windows.Forms;
using System.Threading;

using Microsoft.CSharp;
using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;
using rDSN.Tron.Contract;
using rDSN.Tron.LanguageProvider;

namespace rDSN.Tron.ControlPanel
{
    public class GenerateCompositionStubCommand : Command
    {

        public override bool Execute(List<string> args)
        {
            Console.WriteLine(args);
            var app_name = Path.GetFileNameWithoutExtension(args[2]);
            
            if (SystemHelper.RunProcess("php.exe", Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "bin/dsn.generate_code.php") + " " + args[2] + " csharp tmp binary layer3") != 0)
            {
                return false;
            }
            if (CSharpCompiler.ToDiskAssembly(new string[] {Path.Combine("tmp", app_name + ".Tron.Composition.cs") },
                new string[] { "rDSN.Tron.Utility.dll", "rDSN.Tron.Contract.dll" },
                new string[] { args[2] },
                Path.Combine("tmp", app_name + ".Tron.Composition.dll"),
                false, true
                ))
            {
                Console.WriteLine("Generate success for " + Directory.GetCurrentDirectory() + "\\tmp\\" + app_name + ".Tron.Composition.dll");
            }
            else
            {
                Console.WriteLine("generate composition stub failed");
            }

            return true;
        }

        public override string Help()
        {
            return ".\\Tron.exe gcs thrift dsn .\\add.thrift";
        }
    }
}
