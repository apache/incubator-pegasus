using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace rDSN.Tron.ControlPanel
{
    public class ExitCommand : Command
    {
        public override bool Execute(List<string> args)
        {
            Environment.Exit(0);
            return true;
        }

        public override string Help()
        {
            return "[q]uit - quit the program";
        }
    }
}
