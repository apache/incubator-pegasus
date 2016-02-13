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
