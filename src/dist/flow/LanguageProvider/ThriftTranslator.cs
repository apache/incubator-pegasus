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
    public class ThriftTranslator : IdlTranslator
    {
        public ThriftTranslator() : base()
        {
            specType = ServiceSpecType.Thrift_0_9;
        }

        public ThriftTranslator(ServiceSpecType t) : base(t) { }

        public override bool ToCommonInterface(string dir, string file, string outDir, List<string> args, bool needCompiled = false)
        {
            var compilerPath = LanguageHelper.GetCompilerPath(GetType());
            if (compilerPath == "")
            {
                return false;
            }
            if (args == null)
            {
                args = new List<string>();
            }
            var arguments = "--gen csharp_mini " + string.Join(" ", args) + " " + Path.Combine(dir, file);
            if (SystemHelper.RunProcess(compilerPath, arguments) == 0)
            {
                return true;
                
            }
            else
            {
                return false;
            }
        }

      

    }
}
