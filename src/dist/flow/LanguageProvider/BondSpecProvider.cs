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
using System.Diagnostics;
using System.Linq;

using rDSN.Tron.Contract;
using rDSN.Tron.Utility;

namespace rDSN.Tron.LanguageProvider
{
    class BondSpecProvider : ISpecProvider
    {
        public new ServiceSpecType GetType()
        {
            return ServiceSpecType.bond_3_0;
        }

        private enum GeneratedFileType
        {
            IDL = 0,
            IDL_NoSerivce = 1,
            TYPES = 2,
            PROXIES = 3,
            SERVICES = 4,
            CLIENTS = 5
        }

        private string[] _postfix = { "_IDL.cs", "_IDL_NoService.cs", "_types.cs", "_proxies.cs", "_services.cs", "_Client.cs" };

        private string[] FromSpecToSources(string specFile, string outputDir, GeneratedFileType[] types)
        {
            var compiler = LanguageHelper.GetCompilerPath(ServiceSpecType.bond_3_0);
            var bondc_dir = Path.GetDirectoryName(compiler);
            string[] templates = { "Rules_Bond_CSharp_Client.tt" };
            var arguments = specFile + " /c#" +
                templates.VerboseCombine(" ", t => " /T:" + Path.Combine(bondc_dir, t))
                + " /O:" + outputDir;

            var err = SystemHelper.RunProcess(compiler, arguments);
            Trace.Assert(err == 0, "bondc code generation failed");

            List<string> sources = new List<string>();
            foreach (var t in types)
            {
                string fname = Path.Combine(outputDir, Path.GetFileNameWithoutExtension(specFile) + _postfix[(int)t]);
                Trace.Assert(File.Exists(fname));

                sources.Add(fname);
            }

            return sources.ToArray();
        }

        private string[] FromAllSpecToSources(string mainSpecFile, string[] refSpecFiles, string outputDir, GeneratedFileType[] typesForMain, GeneratedFileType[] typesForReferenced)
        {
            List<string> sources = new List<string>();

            sources.AddRange(FromSpecToSources(mainSpecFile, outputDir, typesForMain));

            foreach (var refSpecFile in refSpecFiles)
            {
                sources.AddRange(FromSpecToSources(refSpecFile, outputDir, typesForReferenced));
            }

            return sources.ToArray();
        }

        //public string[] ToCommonSpec(ServiceSpec spec, string dir)
        //{
        //    return FromAllSpecToSources(
        //                Path.Combine(spec.Directory, spec.MainSpecFile),
        //                spec.ReferencedSpecFiles.Select(rs => Path.Combine(spec.Directory, rs)).ToArray(),
        //                dir,
        //                new GeneratedFileType[] { GeneratedFileType.IDL },
        //                new GeneratedFileType[] { GeneratedFileType.IDL_NoSerivce }
        //                );
        //}

        public void GenerateClientCall(
            CodeBuilder builder,
            MethodCallExpression call, 
            Service svc,
            Dictionary<Type, string> reWrittenTypes
            )
        {
            var argTypeName = call.Method.GetParameters()[0].Name;
            builder.AppendLine(svc.TypeName() + "." + argTypeName + "_result resp;");
            builder.AppendLine((call.Object as MemberExpression).Member.Name + "." + call.Method.Name + "(new " + svc.TypeName() + "." + call.Method.Name + "_args(){" + argTypeName + "= req}, out resp);");
            builder.AppendLine("return resp.Success;");
        }

        public FlowErrorCode GenerateServiceClient(
            ServiceSpec spec,
            string dir,
            ClientLanguage lang,
            ClientPlatform platform,
            out LinkageInfo linkInfo
            )
        {
            var compiler = LanguageHelper.GetCompilerPath(GetType());
            linkInfo = new LinkageInfo();
            if (compiler == "")
            {

                return FlowErrorCode.SpecCompilerNotFound;
            }

            if (platform != ClientPlatform.Windows)
            {
                Console.WriteLine("Bond compiler only supports windows platform!");
                return FlowErrorCode.PlatformNotSupported;
            }


            // hack for C# for the time being
            if (lang == ClientLanguage.Client_CSharp)
            {
                linkInfo.IncludeDirectories.Add(dir);
                linkInfo.LibraryPaths.Add(Directory.GetParent(compiler).FullName);
                linkInfo.LibraryPaths.Add(dir);

                linkInfo.DynamicLibraries.AddRange(new List<string>()
                            {
                                "Microsoft.Bond.dll",
                                "Microsoft.Bond.Rpc.dll",
                                "Microsoft.Bond.Interfaces.dll",
                                "Microsoft.Bond.TypeProvider.dll",
                                "rDSN.Tron.Utility.dll",
                            });

                linkInfo.Sources.AddRange(FromAllSpecToSources(
                        Path.Combine(spec.Directory, spec.MainSpecFile),
                        spec.ReferencedSpecFiles.Select(rs => Path.Combine(spec.Directory, rs)).ToArray(),
                        dir,
                        new[] { GeneratedFileType.TYPES, GeneratedFileType.PROXIES, GeneratedFileType.CLIENTS },
                        new[] { GeneratedFileType.TYPES }
                    )
                    .Select(Path.GetFileName)
                    .ToList());

                return FlowErrorCode.Success;
            }

            List<string> arguments = new List<string>
            {
                " ",
                "/" + GetLanguageName(lang),
                "/I:" + spec.Directory,
                "/O:" + dir,
                Path.Combine(spec.Directory, spec.MainSpecFile)
            };



            if (SystemHelper.RunProcess(compiler, string.Join(" ", arguments)) == 0)
            {

                switch (lang)
                {
                    case ClientLanguage.Client_CSharp:
                        {
                            linkInfo.IncludeDirectories.Add(dir);
                            linkInfo.LibraryPaths.Add(Directory.GetParent(compiler).FullName);
                            linkInfo.LibraryPaths.Add(dir);

                            linkInfo.DynamicLibraries.AddRange(new List<string>()
                            {
                                "Microsoft.Bond.dll",
                                "Microsoft.Bond.Rpc.dll",
                                "Microsoft.Bond.Interfaces.dll",
                                "Microsoft.Bond.TypeProvider.dll",
                            });
                            var specName = Path.GetFileNameWithoutExtension(spec.MainSpecFile);
                            var searchPattern = specName + "_*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15).Select(Path.GetFileName));
                            break;
                        }

                    case ClientLanguage.Client_CPlusPlus:
                        {
                            var bondHeaders = Path.GetPathRoot(compiler);
                            linkInfo.IncludeDirectories.Add(dir);
                            linkInfo.IncludeDirectories.Add(bondHeaders);
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15));
                            break;
                        }
                    case ClientLanguage.Client_Java:
                        {
                            // FIXME: when generate java code, bondc will generate a subdir
                            linkInfo.IncludeDirectories.Add(dir);
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15));
                            break;
                        }
                    default:
                        break;

                }

                return FlowErrorCode.Success;
            }
            else
            {
                return FlowErrorCode.ExceptionError;
            }
        }

        public FlowErrorCode GenerateServiceSketch(
                ServiceSpec spec,
                string dir,
                ClientLanguage lang,
                ClientPlatform platform,
                out LinkageInfo linkInfo
                )
        {
            var compiler = LanguageHelper.GetCompilerPath(GetType());
            linkInfo = new LinkageInfo();
            if (compiler == "")
            {

                return FlowErrorCode.SpecCompilerNotFound;
            }

            if (platform != ClientPlatform.Windows)
            {
                Console.WriteLine("Bond compiler only supports windows platform!");
                return FlowErrorCode.PlatformNotSupported;
            }


            // hack for C# for the time being
            if (lang == ClientLanguage.Client_CSharp)
            {
                linkInfo.IncludeDirectories.Add(dir);
                linkInfo.LibraryPaths.Add(Directory.GetParent(compiler).FullName);
                linkInfo.LibraryPaths.Add(dir);

                linkInfo.DynamicLibraries.AddRange(new List<string>()
                            {
                                "Microsoft.Bond.dll",
                                "Microsoft.Bond.Rpc.dll",
                                "Microsoft.Bond.Interfaces.dll",
                                "Microsoft.Bond.TypeProvider.dll",
                            });
                linkInfo.Sources = FromAllSpecToSources(
                        Path.Combine(spec.Directory, spec.MainSpecFile),
                        spec.ReferencedSpecFiles.Select(rs => Path.Combine(spec.Directory, rs)).ToArray(),
                        dir,
                        new[] { GeneratedFileType.TYPES, GeneratedFileType.SERVICES },
                        new[] { GeneratedFileType.TYPES }
                    )
                    .Select(Path.GetFileName)
                    .ToList();

                return FlowErrorCode.Success;
            }


            List<string> arguments = new List<string>
            {
                " ",
                "/" + GetLanguageName(lang),
                "/I:" + spec.Directory,
                "/O:" + dir,
                Path.Combine(spec.Directory, spec.MainSpecFile)
            };



            if (SystemHelper.RunProcess(compiler, string.Join(" ", arguments)) == 0)
            {

                switch (lang)
                {
                    case ClientLanguage.Client_CSharp:
                        {
                            linkInfo.IncludeDirectories.Add(dir);
                            linkInfo.LibraryPaths.Add(Directory.GetParent(compiler).FullName);
                            linkInfo.LibraryPaths.Add(dir);

                            linkInfo.DynamicLibraries.AddRange(new List<string>()
                            {
                                "Microsoft.Bond.dll",
                                "Microsoft.Bond.Rpc.dll",
                                "Microsoft.Bond.Interfaces.dll",
                                "Microsoft.Bond.TypeProvider.dll",
                            });
                            var specName = Path.GetFileNameWithoutExtension(spec.MainSpecFile);
                            var searchPattern = specName + "_*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15).Select(Path.GetFileName));
                            break;
                        }

                    case ClientLanguage.Client_CPlusPlus:
                        {
                            var bondHeaders = Path.GetPathRoot(compiler);
                            linkInfo.IncludeDirectories.Add(dir);
                            linkInfo.IncludeDirectories.Add(bondHeaders);
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15));
                            break;
                        }
                    case ClientLanguage.Client_Java:
                        {
                            // FIXME: when generate java code, bondc will generate a subdir
                            linkInfo.IncludeDirectories.Add(dir);
                            var searchPattern = "*." + LanguageHelper.GetSourceExtension(lang);
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15));
                            break;
                        }
                    default:
                        break;

                }

                return FlowErrorCode.Success;
            }
            else
            {
                return FlowErrorCode.ExceptionError;
            }
        }


        private string GetLanguageName(ClientLanguage lang)
        {
            Dictionary<ClientLanguage, string> map = new Dictionary<ClientLanguage, string>()
            {
                {ClientLanguage.Client_CPlusPlus, "c++"},
                {ClientLanguage.Client_CSharp, "c#"},
                {ClientLanguage.Client_Java, "java"},
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
