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
using rDSN.Tron.Utility;


namespace rDSN.Tron.ControlPanel
{
    public class GenerateCompositionStubCommand : Command
    {
        public override bool Execute(List<string> args)
        {
            SystemHelper.CreateOrCleanDirectory("tmp");
            SystemHelper.RunProcess("php.exe",
                 string.Join(" ", Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "bin", "dsn.templates", "csharp", "composition", "gen_composition_stub.php"), string.Join(" ", args.Where(a => a.EndsWith(".proto") || a.EndsWith(".thrift")).ToArray())));
            
            CSharpCompiler.ToDiskAssembly(
                Directory.GetFiles("tmp", "*.cs", SearchOption.AllDirectories).ToArray(),
                new[] { "rDSN.Tron.Utility.dll", "rDSN.Tron.Contract.dll" },
                Directory.GetFiles("tmp", "*.proto", SearchOption.AllDirectories).Concat(
                    args.Where(a => a.EndsWith(".thrift"))).ToArray(),
                "compo.dll");
            Console.ReadLine();
            return true;
        }

        public override string Help()
        {
            return ".\\Tron.exe gcs 1.thrift 2.proto 3.thrift";
        }
    }
}
