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
using System.Linq.Expressions;
using rDSN.Tron.Contract;
using rDSN.Tron.Utility;

namespace rDSN.Tron.LanguageProvider
{

    class ProtoSpecProvider : ISpecProvider
    {
        public new ServiceSpecType GetType()
        {
            return ServiceSpecType.proto;

        }

        //public string[] ToCommonSpec(ServiceSpec spec, string dir)
        //{
        //    throw new NotImplementedException();

        //}

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


        public FlowErrorCode GenerateServiceClient(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            if (!spec.IsRdsnRpc) throw new NotImplementedException();
            linkInfo = new LinkageInfo();
            var appName = Path.GetFileNameWithoutExtension(spec.MainSpecFile);
            if (
                SystemHelper.RunProcess("php.exe",
                     Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "bin/dsn.generate_code.php") + " " +
                    Path.Combine(spec.Directory, spec.MainSpecFile) + " csharp " + dir + " binary layer3") != 0)
                return FlowErrorCode.ProcessStartFailed;
            linkInfo.Sources.Add(Path.Combine(dir, "GProtoBinaryHelper.cs"));
            linkInfo.Sources.Add(Path.Combine(dir, appName + ".cs"));
            linkInfo.Sources.Add(Path.Combine(dir, appName + ".client.cs"));
            linkInfo.Sources.Add(Path.Combine(dir, appName + ".code.definition.cs"));
            linkInfo.DynamicLibraries.Add(Path.Combine(Environment.GetEnvironmentVariable("DSN_ROOT"), "lib", "Google.Protobuf.dll"));
            linkInfo.DynamicLibraries.Add(Path.Combine("System.IO.dll"));
            linkInfo.DynamicLibraries.Add(Path.Combine("System.runtime.dll"));
            return FlowErrorCode.Success;
        }


        public FlowErrorCode GenerateServiceSketch(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            linkInfo = null;
            return FlowErrorCode.Success;
        }

        public void GenerateClientCall(CodeBuilder builder, MethodCallExpression call, Service svc, Dictionary<Type, string> reWrittenTypes)
        {
            builder.AppendLine(call.Type.GetCompilableTypeName(reWrittenTypes) + " resp;");
            builder.AppendLine((call.Object as MemberExpression).Member.Name + "." + call.Method.Name + "(req, out resp);");
            builder.AppendLine("return resp;");
        }
    }

}
