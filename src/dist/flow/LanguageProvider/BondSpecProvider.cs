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

    class BondSpecProvider : ISpecProvider
    {
        public new ServiceSpecType GetType()
        {
            return ServiceSpecType.Bond_3_0;

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
            var prefix = "..\\..\\external\\bond\\";
            string[] templates = {"Rules_Bond_CSharp_IDL.tt", "Rules_Bond_CSharp_IDL_NoService.tt", "Rules_Bond_CSharp_Client.tt"};
            var fileName = prefix + "amd64\\bondc.exe";
            var arguments = specFile + " /c#" + 
                templates.VerboseCombine(" ", t => " /T:" + prefix + t)
                + " /O:" + outputDir;
            int err = SystemHelper.RunProcess(fileName, arguments);
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
        
        public string[] ToCommonSpec(ServiceSpec spec, string dir)
        {
            return FromAllSpecToSources(
                        Path.Combine(spec.Directory, spec.MainSpecFile),
                        spec.ReferencedSpecFiles.Select(rs => Path.Combine(spec.Directory, rs)).ToArray(),
                        dir,
                        new GeneratedFileType[] { GeneratedFileType.IDL },
                        new GeneratedFileType[] { GeneratedFileType.IDL_NoSerivce }
                        );            
        }

   
        public ErrorCode GenerateServiceClient(
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

                return ErrorCode.SpecCompilerNotFound;
            }

            if (platform != ClientPlatform.Windows)
            {
                Console.WriteLine("Bond compiler only supports windows platform!");
                return ErrorCode.PlatformNotSupported;
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
                                "rDSN.Tron.Runtime.Common.dll",
                            });
                linkInfo.Sources = FromAllSpecToSources(
                        Path.Combine(spec.Directory, spec.MainSpecFile),
                        spec.ReferencedSpecFiles.Select(rs => Path.Combine(spec.Directory, rs)).ToArray(),
                        dir,
                        new GeneratedFileType[] { GeneratedFileType.TYPES, GeneratedFileType.PROXIES, GeneratedFileType.CLIENTS },
                        new GeneratedFileType[] { GeneratedFileType.TYPES }
                    )
                    .Select(s => Path.GetFileName(s))
                    .ToList();

                return ErrorCode.Success;
            }            
            
            List<string> arguments = new List<string>();

            arguments.Add(" ");
            arguments.Add("/" + GetLanguageName(lang));
            arguments.Add("/I:" + spec.Directory);
            arguments.Add("/O:" + dir);
            arguments.Add(Path.Combine(spec.Directory, spec.MainSpecFile));

            
            if (SystemHelper.RunProcess(compiler, string.Join(" ", arguments)) == 0)
            {
              
                switch(lang)
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
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15).Select(f => Path.GetFileName(f)));
                            break;
                        }

                    case ClientLanguage.Client_CPlusPlus:
                        {
                            var bondHeaders = Path.GetPathRoot(compiler);
                            linkInfo.IncludeDirectories.Add(dir);
                            linkInfo.IncludeDirectories.Add(bondHeaders);
                            var searchPattern =  "*." + LanguageHelper.GetSourceExtension(lang);
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
            var compiler = LanguageHelper.GetCompilerPath(GetType());
            linkInfo = new LinkageInfo();
            if (compiler == "")
            {

                return ErrorCode.SpecCompilerNotFound;
            }

            if (platform != ClientPlatform.Windows)
            {
                Console.WriteLine("Bond compiler only supports windows platform!");
                return ErrorCode.PlatformNotSupported;
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
                        new GeneratedFileType[] { GeneratedFileType.TYPES, GeneratedFileType.SERVICES },
                        new GeneratedFileType[] { GeneratedFileType.TYPES }
                    )
                    .Select(s => Path.GetFileName(s))
                    .ToList();

                return ErrorCode.Success;
            }            
            

            List<string> arguments = new List<string>();

            arguments.Add(" ");
            arguments.Add("/" + GetLanguageName(lang));
            arguments.Add("/I:" + spec.Directory);
            arguments.Add("/O:" + dir);
            arguments.Add(Path.Combine(spec.Directory, spec.MainSpecFile));


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
                            linkInfo.Sources.AddRange(SystemHelper.GetFilesByLastWrite(dir, searchPattern, SearchOption.AllDirectories, 15).Select(f => Path.GetFileName(f)));
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

                return ErrorCode.Success;
            }
            else
            {
                return ErrorCode.ExceptionError;
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
