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
using System.IO;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.ControlPanel
{
    class Program
    {
        static void Main(string[] args)
        {
            // general commands
            CommandManager.Instance().Add(new string[] {"quit", "Quit", "q", "Q"}, new ExitCommand());
            CommandManager.Instance().Add(new string[] { "help", "Help", "h", "H" }, new HelpCommand());            
            CommandManager.Instance().Add(new string[] { "Repeat", "repeat", "r", "R" }, new RepeatCommand());            
            CommandManager.Instance().Add(new string[] { "enum", "Enum", "e", "E" }, new EnumCommand());
            CommandManager.Instance().Add(new string[] { "win", "Win", "w", "W" }, new WinBatchCommand());
            
            // local tools
            CommandManager.Instance().Add(new string[] { "GenCompositionStub", "GCS", "gcs" }, new GenerateCompositionStubCommand());
            CommandManager.Instance().Add(new string[] { "Generatec", "generatec", "gc", "GC" }, new GenerateCommonInterfaceCommand());
            CommandManager.Instance().Add(new string[] { "Generateb", "generateb", "gb", "GB" }, new GenerateIDLFileCommand());
            
            if (args.Length == 0)
            {
                CommandManager.Instance().Run();
            }
            else
            {
                if (File.Exists(args[0]))
                {
                    foreach (var filePath in args)
                    {
                        string line;
                        var filestream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                        var file = new StreamReader(filestream);

                        Console.WriteLine("execute commands in file '" + filePath + "'");

                        while ((line = file.ReadLine()) != null)
                        {
                            line = line.Trim();
                            if (line.Length == 0 || line[0] == '#')
                                continue;

                            CommandManager.Instance().ExecuteCommand(line);
                        }
                    }

                    Console.WriteLine("====== END OF FILE COMMANDS EXECUTION =======");
                }
                else
                {
                    List<string> cargs = new List<string>();
                    if (args.Length > 1)
                    {
                        for (int i = 1; i < args.Length; i++)
                            cargs.Add(args[i]);
                    }

                    CommandManager.Instance().ExecuteCommand(args[0], cargs);

                    Console.WriteLine("====== END OF DIRECT COMMAND EXECUTION =======");
                }

                Environment.Exit(0);
            }
        }
    }
}
