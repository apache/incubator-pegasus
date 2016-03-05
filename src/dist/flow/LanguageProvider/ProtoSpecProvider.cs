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
using System.Reflection;
using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.LanguageProvider
{

    class ProtoSpecProvider : ISpecProvider
    {
        public new ServiceSpecType GetType()
        {
            return ServiceSpecType.Proto_Buffer_1_0;

        }

        public string[] ToCommonSpec(ServiceSpec spec, string dir)
        {
            throw new NotImplementedException();

        }

        //public ServiceSpec FromCommonSpec(string spec, ServiceSpecType targetType)
        //{
        //    var generator = IdlGenerator.GetInstance(targetType);
        //    Assembly asm = Assembly.LoadFrom(spec);
        //    generator.Generate(asm);
        //    ServiceSpec serviceSpec = new ServiceSpec();
        //    // FIXME
        //    serviceSpec.SType = targetType;
        //    serviceSpec.Directory = ".";
        //    serviceSpec.MainSpecFile = "";
        //    serviceSpec.ReferencedSpecFiles = null;
        //    return serviceSpec;
        //}


        public ErrorCode GenerateServiceClient(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            if (spec.IsRdsnRpc)
            {
                linkInfo = new LinkageInfo();
                var app_name = Path.GetFileNameWithoutExtension(spec.MainSpecFile);
                if (SystemHelper.RunProcess("php.exe", Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "bin/dsn.generate_code.php") + " " + Path.Combine(spec.Directory, spec.MainSpecFile) + " csharp " + dir + " binary layer3") == 0)
                {
                    CSharpCompiler.ToDiskAssembly(
                        new string[] { Path.Combine(dir, "GProtoBinaryHelper.cs"), Path.Combine(dir, app_name + ".cs"), Path.Combine(dir, app_name + ".client.cs"), Path.Combine(dir, app_name + ".code.definition.cs") },
                        new string[] { Path.Combine("System.IO.dll"), Path.Combine("System.runtime.dll"), Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "Google.Protobuf.dll"), Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "dsn.dev.csharp.dll")},
                        new string[] { },
                        Path.Combine(dir, app_name + ".client.dll")
                        );
                    linkInfo.DynamicLibraries.Add(Path.Combine(dir, app_name + ".client.dll"));
                    linkInfo.DynamicLibraries.Add(Path.Combine("System.IO.dll"));
                    linkInfo.DynamicLibraries.Add(Path.Combine("System.runtime.dll"));
                    linkInfo.DynamicLibraries.Add(Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "Google.Protobuf.dll"));
                    return ErrorCode.Success;
                }
                else
                {
                    return ErrorCode.ProcessStartFailed;
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }


        public ErrorCode GenerateServiceSketch(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            linkInfo = null;
            return ErrorCode.Success;
        }
    }

}
