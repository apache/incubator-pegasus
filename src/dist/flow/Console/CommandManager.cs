using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class CommandManager : Singleton<CommandManager>
    {
        public void Add(string name, Command cmd)
        {
            if (_commands.ContainsKey(name))
                throw new Exception("command alias '" + name + "' is already registered.");

            _commands.Add(name, cmd);
        }

        public void Add(string[] names, Command cmd)
        {
            foreach (var name in names)
            {
                Add(name, cmd);
            }
        }

        public bool Remove(string name)
        {
            return _commands.Remove(name);
        }

        public void Run()
        {
            while (true)
            {
                Console.Write(">");
                string cmdline = Console.ReadLine();
                ExecuteCommand(cmdline);
            }
        }

        public bool ExecuteCommand(string cmdLine)
        {
            string oldCommand = cmdLine;
            var args = cmdLine.Split(new char[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);

            if (args.Length == 0)
                return false;

            List<string> rargs = new List<string>();
            for (int i = 1; i < args.Length; i++)
            {
                rargs.Add(args[i]);
            }

            return ExecuteCommand(args[0], rargs);
        }

        public bool ExecuteCommand(string name, List<string> args)
        {
            Command cmd = Get(name);
            if (cmd == null)
            {
                Console.WriteLine("invalid command '" + name + "'");
                return false;
            }

            var start = SystemHelper.GetCurrentMillieseconds();
            bool r = false;

            try
            {
                r = cmd.Execute(args);
            }
            catch (Exception e)
            {
                r = false;
                Console.WriteLine("exception, msg = " + e.Message);
            }

            var end = SystemHelper.GetCurrentMillieseconds();

            Console.WriteLine("------------------------------------------");
            Console.WriteLine("exec '" + name + "'" +
                (r ? "succeeded" : "failed") +
                ", elapsed " + (end - start) + " ms"
                );
            return true;
        }

        public void ExecuteCommands(List<string> cmds)
        {
            foreach (var cmd in cmds)
            {
                ExecuteCommand(cmd);
            }
        }

        public void ExecuteCommands(string cmdFile)
        {
            try
            {
                StreamReader reader = new StreamReader(cmdFile);
                string cmd;
                while ((cmd = reader.ReadLine()) != null)
                {
                    ExecuteCommand(cmd);
                }
                reader.Close();
            }
            catch (Exception)
            { }
        }

        public Dictionary<string, Command> Commands { get { return _commands; } }

        public Command Get(string name)
        {
            Command cmd = null;
            _commands.TryGetValue(name, out cmd);
            return cmd;
        }
        private Dictionary<string, Command> _commands = new Dictionary<string,Command>();
    }
}
