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
