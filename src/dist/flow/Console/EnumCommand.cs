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
using System.Threading.Tasks;

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class EnumCommand : Command
    {
        private static HashSet<string> _blackList = new HashSet<string>(
            Configuration.Instance().GetSection("BlackList") != null ? 
            Configuration.Instance().GetSection("BlackList").Select(k => k.Key) :
            new string[]{}
            );

        public override bool Execute(List<string> args)
        {
            if (args.Count < 2)
                return false;

            string elementList = args[0];
            string cmd = args[1];

            List<string> cargs = new List<string>();
            if (args.Count > 2)
            {
                for (int i = 2; i < args.Count; i++)
                    cargs.Add(args[i]);
            }

            string[] elements;
            // pre[min:max]post
            if (elementList.Contains('[') && elementList.Contains(']'))
            {
                int lpos = elementList.IndexOf('[');
                int rpos = elementList.IndexOf(']');
                string prefix = elementList.Substring(0, lpos);
                string postfix = elementList.Substring(rpos + 1);
                string range = elementList.Substring(lpos + 1, rpos - lpos - 1);
                int colonPos = range.IndexOf(':');
                int min = int.Parse(range.Substring(0, colonPos));
                int max = int.Parse(range.Substring(colonPos + 1));

                string fmt = "0";
                if (max < 10)
                    fmt = "0";
                else if (max < 100)
                    fmt = "00";
                else if (max < 1000)
                    fmt = "000";
                else if (max < 10000)
                    fmt = "0000";
                else
                    throw new Exception("too big");

                List<string> es = new List<string>();

                for (int i = min; i <= max; i++)
                {
                    string e = prefix + i.ToString(fmt) + postfix;
                    es.Add(e);
                }

                elements = es.ToArray();
            }
            else
            {
                elements = elementList.Split(new char[] { ',' });
            }

            var tasks = new List<KeyValuePair<Task, string>>();
            var completedTasks = new List<KeyValuePair<Task, string>>();

            DateTime beginTs = DateTime.Now;

            foreach (var e in elements)
            {
                if (_blackList.Contains(e))
                    continue;

                var myargs = cargs.Select(a => 
                {
                    if (a == "%m" || a == "%M") return e;
                    else return a;
                }).ToList();

                Task<int> task = new Task<int>(
                        () =>
                        {
                            //Console.WriteLine("execute command '" + cmd + " " + myargs.VerboseCombine(" ", a => a));
                            CommandManager.Instance().ExecuteCommand(args[1], myargs);
                            //Console.WriteLine();
                            return 0;
                        }
                    );

                tasks.Add(new KeyValuePair<Task, string>(task, e));
                task.Start();
            }

            int totalTaskCount = tasks.Count;
            int completeCount = 0;

            while (completeCount < totalTaskCount)
            {
                Thread.Sleep(1000);
                foreach (var task in tasks)
                {
                    if (task.Key.Wait(1))
                    {
                        completedTasks.Add(task);
                    }
                }

                completeCount += completedTasks.Count();
                foreach (var task in completedTasks)
                {
                    tasks.Remove(task);
                }
                completedTasks.Clear();

                Console.Write(" " + completeCount + " in " + totalTaskCount + " tasks done");
                if (completeCount > 0.7 * totalTaskCount && tasks.Count > 0)
                {
                    Console.Write(", " + (totalTaskCount - completeCount) + " remaining: ");
                    foreach (var task in tasks)
                    {
                        Console.Write(task.Value + ", ");
                    }
                }
                Console.WriteLine();
            }

            return true;
        }

        public override string Help()
        {
            return "[e|E]num m0,m1,m2|pre[min:max]post commandtemplate(%m as the place holder)\n"
                + "\t%m as the placeholder for m0,m1, ...\n"
                + "\t'e srgsi-[0:10] s %m stop' to stop app server on machine srgsi-00~srgsi-10\n"
                +"\t'e srgsi-00,srgsi-01 s %m stop' to stop app server on machine srgsi-00 and srgsi-01";
        }

        public override string Usage() { return Help(); }
    }
}
