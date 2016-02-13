using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class WinBatchCommand : Command
    {
        public override bool Execute(List<string> args)
        {
            Console.WriteLine(SystemHelper.RunCommand(args.VerboseCombine(" ", a => a)));
            return true;
        }

        public override string Help()
        {
            return "[w|W]in windows command line command";
        }
    }
}
