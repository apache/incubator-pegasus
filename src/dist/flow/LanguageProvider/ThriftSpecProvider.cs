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

    class ThriftSpecProvider : ISpecProvider
    {
        public new ServiceSpecType GetType()
        {
            return ServiceSpecType.Thrift_0_9;
        }

        public string[] ToCommonSpec(ServiceSpec spec, string dir)
        {
            var translator = IdlTranslator.GetInstance(GetType());
            var inputDir = spec.Directory;
            var file = spec.MainSpecFile;
            var outDir = dir;
            var args = new List<string>() 
            {
                "-out " + outDir,
                "-r" // recursively generate all included files
            };
            if (translator.ToCommonInterface(inputDir, file, outDir, args))
            {
                int threshhold = 30;  // filter the .cs files by their LastWriteTimes
                var output = SystemHelper.GetFilesByLastWrite(outDir, "*_common.cs", SearchOption.TopDirectoryOnly, threshhold);
                return output.ToArray();
            }
            else
            {
                return null;
            }
        }

        public ErrorCode GenerateServiceClient(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            var compiler = LanguageHelper.GetCompilerPath(GetType(), platform);
            linkInfo = new LinkageInfo();
            if (compiler == "")
            {

                return ErrorCode.SpecCompilerNotFound;
            }


            List<string> arguments = new List<string>();
            var languageName = GetLanguageName(lang);

            arguments.Add(" ");
            arguments.Add("--" + languageName);
            arguments.Add("-r");
            arguments.Add("-out " + dir);
            arguments.Add(Path.Combine(spec.Directory, spec.MainSpecFile));
            if (SystemHelper.RunProcess(compiler, string.Join(" ", arguments)) == 0)
            {
                // generally, thrift.exe will generate a folder in the name of the mainspec's namespace to the output dir,e.g. gen-csharp
                // all language libraries are availabe in the source code of thrift project, placed in the thrift\\lib\\{language} dir
                // in Tron project, we place thrift compiler at "external\\thrift\\bin", and place the libraries in at"external\\thrift\\lib\\{language}"
                switch (lang)
                {
                    case ClientLanguage.Client_CSharp:
                        {
                            var sourceDir = Path.Combine(dir, "gen-" + languageName);
                            linkInfo.IncludeDirectories.Add(sourceDir);
                            linkInfo.LibraryPaths.Add(Path.Combine(Directory.GetParent(compiler).FullName, "lib\\csharp"));
                            linkInfo.LibraryPaths.Add(dir);

                            linkInfo.DynamicLibraries.AddRange(new List<string>()
                            {
                                "Thrift.dll"
                            });
                            var specName = Path.GetFileNameWithoutExtension(spec.MainSpecFile);
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(sourceDir, searchPattern, SearchOption.AllDirectories, 15).Select(f => Path.GetFileName(f)));
                            break;
                        }

                    case ClientLanguage.Client_CPlusPlus:
                        {
                            var sourceDir = Path.Combine(dir, "gen-" + languageName);
                            linkInfo.IncludeDirectories.Add(sourceDir);
                            linkInfo.LibraryPaths.Add(sourceDir);
                            linkInfo.LibraryPaths.Add(Path.Combine(Directory.GetParent(compiler).FullName, "lib\\cpp"));                            
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(sourceDir, searchPattern, SearchOption.AllDirectories, 15));
                            break;
                        }
                    case ClientLanguage.Client_Java:
                        {
                            var sourceDir = Path.Combine(dir, "gen-" + languageName);
                            linkInfo.IncludeDirectories.Add(sourceDir);
                            linkInfo.LibraryPaths.Add(sourceDir);
                            linkInfo.LibraryPaths.Add(Path.Combine(Directory.GetParent(compiler).FullName, "lib\\java"));
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(sourceDir, searchPattern, SearchOption.AllDirectories, 15));
                            break;
                        }
                    default:
                        break;

                }

                return ErrorCode.Success;
            }
            else
            {
                return ErrorCode.ExceptionError;
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
            return ErrorCode.NotImplemented;
        }


        private string GetLanguageName(ClientLanguage lang)
        {
            Dictionary<ClientLanguage, string> map = new Dictionary<ClientLanguage, string>()
            {
                {ClientLanguage.Client_CPlusPlus, "cpp"},
                {ClientLanguage.Client_CSharp, "csharp"},
                {ClientLanguage.Client_Java, "java"},
                {ClientLanguage.Client_Javascript, "js"},
                {ClientLanguage.Client_Python, "py"},
            };
            if (map.ContainsKey(lang))
            {
                return map[lang];
            }
            else
            {
                return "";
            }
        }





    }

}
