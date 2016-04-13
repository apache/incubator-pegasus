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

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class CommandManager : Singleton<CommandManager>
    {
        public void Add(string name, Command cmd)
        {
            if (Commands.ContainsKey(name))
                throw new Exception("command alias '" + name + "' is already registered.");

            Commands.Add(name, cmd);
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
            return Commands.Remove(name);
        }

        public void Run()
        {
            while (true)
            {
                Console.Write(">");
                var cmdline = Console.ReadLine();
                ExecuteCommand(cmdline);
            }
        }

        public bool ExecuteCommand(string cmdLine)
        {
            var args = cmdLine.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);

            if (args.Length == 0)
                return false;

            var rargs = new List<string>();
            for (var i = 1; i < args.Length; i++)
            {
                rargs.Add(args[i]);
            }

            return ExecuteCommand(args[0], rargs);
        }

        public bool ExecuteCommand(string name, List<string> args)
        {
            var cmd = Get(name);
            if (cmd == null)
            {
                Console.WriteLine("invalid command '" + name + "'");
                return false;
            }

            var start = SystemHelper.GetCurrentMillieseconds();
            bool r;

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
                var reader = new StreamReader(cmdFile);
                string cmd;
                while ((cmd = reader.ReadLine()) != null)
                {
                    ExecuteCommand(cmd);
                }
                reader.Close();
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public Dictionary<string, Command> Commands { get; } = new Dictionary<string,Command>();

        public Command Get(string name)
        {
            Command cmd;
            Commands.TryGetValue(name, out cmd);
            return cmd;
        }
    }
}
