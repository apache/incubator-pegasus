using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.ControlPanel
{
    public class HelpCommand : Command
    {
        public override bool Execute(List<string> args)
        {
            if (args.Count == 0)
	        {
		        Console.WriteLine("for detailed cmd usage, please use help 'cmd'");
                Console.WriteLine("---------------------------------------------");

                foreach (var cmd in CommandManager.Instance().Commands.Select(c => c.Value).Distinct())
                {
                    Console.WriteLine(cmd.Help());
                }
	        }

            foreach (var name in args)
            {            
		        Command cmd = CommandManager.Instance().Get(name);
                Console.WriteLine("");
		        if (cmd != null)
		        {			
			        Console.WriteLine(cmd.Usage());
		        }
		        else
		        {
                    Console.WriteLine("'" + name + "' is an unknown command");
		        }
	        }

	        return true;
        }

        public override string Help()
        {
            return "[h]elp - show help info";
        }
    }
}
