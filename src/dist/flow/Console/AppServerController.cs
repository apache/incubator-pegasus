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
using System.Diagnostics;
using System.Threading;

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class AppServerController : Command
    {
        public override bool Execute(List<string> args)
        {
            if (args.Count < 2)
                return false;

            // return "[s|S]erver machine deploy|start|stop|clear [verbose = false] [faked = false] [dir = d:\\" + System.Environment.UserName + "\\Tron\\AppServer] \n";
            string machine = args[0];
            string command = args[1];            

            bool verbose = false;
            if (args.Count > 2)
                verbose = bool.Parse(args[2]);

            bool faked = false;
            if (args.Count > 3)
                faked = bool.Parse(args[3]);

            string dir = "d:\\" + System.Environment.UserName + "\\Tron\\AppServer";
            if (args.Count > 4)
                dir = args[4];
            
            DeployHelper.IsFaked = faked;

            switch (command)
            {
                case "deploy":
                case "Deploy":
                    Deploy(machine, dir, verbose);
                    break;

                case "start":
                case "Start":
                    Start(machine, dir, verbose);
                    break;

                case "Stop":
                case "stop":
                    Stop(machine, verbose);
                    break;

                case "clear":
                case "Clear":
                    Clear(machine, dir, verbose);
                    break;

                default:
                    Console.WriteLine("unknown command '" + command + "'");
                    break;
            }            
            return true;
        }

        private static HashSet<string> _resources = new HashSet<string>() 
        {
            "AppServer.exe",
            "rDSN.Tron.Utility.dll",
            "rDSN.Tron.Contract.dll",
            "rDSN.Tron.Runtime.FailureDetector.dll",
            "rDSN.Tron.Runtime.Common.dll",
            "Microsoft.Bond.dll",
            "Microsoft.Bond.Rpc.dll",
            "Microsoft.Bond.Interfaces.dll",
            "config.ini",
        };

        private void Deploy(string machineName, string dir, bool verbose)
        {
            foreach (var dr in SystemHelper.LeveledDirs(dir))
            {
                DeployHelper.RunRemoteCommand(machineName, "cmd.exe /c \"mkdir " + dr + "\"", verbose);
                Thread.Sleep(500);
            }

            // copy bits
            foreach (var s in _resources)
            {
                DeployHelper.RemoteCopy(s, machineName, dir + "\\", verbose);
            }

            // firwall
            DeployHelper.EnableFirewallRule("TronAppServer", dir + "\\" + "AppServer.exe", machineName, verbose);
        }

        private void Start(string machineName, string dir, bool verbose)
        {
            DeployHelper.RunRemoteCommand(machineName, "cmd.exe /c START AppServer.exe", verbose, dir);
        }

        private void Stop(string machineName, bool verbose)
        {
            string cmd = @"TASKKILL /S " + machineName + " /F /IM AppServer.exe";
            DeployHelper.RunCommand(cmd, verbose);
        }

        private void Clear(string machineName, string dir, bool verbose)
        {
            Stop(machineName, verbose);
            Thread.Sleep(1000);

            string cmd = "cmd.exe /c \"RMDIR /S /Q " + dir;
            cmd += " & " + @"RMDIR /S /Q " + dir + "\"";

            //cmd += " & " + (@"DEL /F " + RemoteDataDir + @"\..\minidumps\" + FileName + "*").PathClean();
            //cmd += " & " + (@"DEL /F " + RemoteDataDir + @"\..\logs\local\" + FileName + "*").PathClean();

            DeployHelper.RunRemoteCommand(machineName, cmd, verbose);
        }

        public override string Help()
        {
            return "[s|S]erver machine deploy|start|stop|clear [verbose = false] [faked = false] [dir = d:\\" + System.Environment.UserName + "\\Tron\\AppServer]";
        }

        public override string Usage() { return Help(); }
    }
}
