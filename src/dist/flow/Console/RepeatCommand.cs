using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class RepeatCommand : Command
    {
        public override bool Execute(List<string> args)
        {
            if (args.Count < 2)
                return false;

            List<string> cargs = new List<string>();
            if (args.Count > 2)
            {
                for (int i = 2; i < args.Count; i++)
                    cargs.Add(args[i]);
            }

            if (args[0][0] == 'c' || args[0][0] == 'C')
            {
                for (int i = 0; i < int.Parse(args[0].Substring(1)); i++)
                {
                    Console.WriteLine("execute command '" + args[1] + " " + cargs.VerboseCombine(" ", a => a) + "' for the " + i + " time");
                    CommandManager.Instance().ExecuteCommand(args[1], cargs);
                    Console.WriteLine();
                }
            }
            else if (args[0][0] == 'i' || args[0][0] == 'I')
            {
                int seconds = int.Parse(args[0].Substring(1));
                while (true)
                {
                    Console.WriteLine(DateTime.Now.ToLongTimeString() + ": execute command '" + args[1] + " " + cargs.VerboseCombine(" ", a => a) + "'");
                    CommandManager.Instance().ExecuteCommand(args[1], cargs);
                    Console.WriteLine();
                    Thread.Sleep(seconds * 1000);
                }
            }
            else
                return false;

            return true;
        }

        public override string Help()
        {
            return "[r|R]epeat cN|iN command\n"
                + "\tcN: execute N times\n"
                + "\tiN: execute for each N seconds";
        }

        public override string Usage() { return Help(); }
    }
}
