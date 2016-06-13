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
using System.Diagnostics;

namespace rDSN.Tron.Utility
{
    public enum RemotingTool
    { 
        None,
        PsExec,
        ApServ
    }

    public class DeployHelper
    {
        public static bool IsFaked = false;
        public static RemotingTool Remoting = RemotingTool.PsExec;

        public static void EnableFirewallRule(string name, string programFullpath, string machineName, bool verbose = false)
        {
            programFullpath = programFullpath.PathClean();

            RunRemoteCommand(machineName, "netsh advfirewall firewall delete rule dir=in name=" + name + "In", verbose);

            RunRemoteCommand(machineName, "netsh advfirewall firewall delete rule dir=out name=" + name + "Out", verbose);

            RunRemoteCommand(machineName, "netsh advfirewall firewall add rule dir=in enable=yes name=" + name + "In action=allow program=" + programFullpath, verbose);

            RunRemoteCommand(machineName, "netsh advfirewall firewall add rule dir=out enable=yes name=" + name + "Out action=allow program=" + programFullpath, verbose);
        }

        public static void RemoteRoboCopy(string localFullPath, string remoteMachine, string remoteFullPath, bool cleanup = false, bool verbose = false)
        {
            localFullPath = localFullPath.PathClean();
            remoteFullPath = remoteFullPath.PathClean();

            var rpath = "\\\\" + remoteMachine + "\\" + remoteFullPath.Replace(':', '$');
            RunCommand("robocopy " + (cleanup ? "/MIR" : "") + " " + localFullPath + " " + rpath + (cleanup ? "" : " /S"), verbose);
        }

        public static void RemoteCopy(string localFullPath, string remoteMachine, string remoteFullPath, bool verbose = false)
        {
            localFullPath = localFullPath.PathClean();
            remoteFullPath = remoteFullPath.PathClean();

            var rpath = "\\\\" + remoteMachine + "\\" + remoteFullPath.Replace(':', '$');
            RunCommand("copy /Y " + localFullPath + " " + rpath, verbose);
        }

        public static void StartService(string machineName, string workingDir, string binary, string arguments, bool verbose = false)
        {
            workingDir = workingDir.PathClean();

            RunRemoteCommand(machineName, "cd /d " + workingDir + " & start " + binary + " " + arguments, verbose);
        }

        public static void StopService(string machineName, string binary, bool verbose = false)
        {
            RunRemoteCommand(machineName, "TASKKILL /F /IM " + binary, verbose);
        }

        public static void RunRemoteCommand(string machineName, string cmd, bool verbose = false, string workdir = "")
        {
            if (machineName.ToLower() == Environment.MachineName.ToLower() || machineName.ToLower() == "localhost")
                RunCommand(cmd, verbose);
            else if (Remoting ==  RemotingTool.PsExec)
            {
                if (workdir == "")
                {
                    RunCommand("psexec.exe \\\\" + machineName + " " + cmd + "", verbose);
                }
                else
                {
                    RunCommand("psexec.exe \\\\" + machineName + " -w " + workdir + " " + cmd + "", verbose);
                }
            }
            else
            {
                Trace.Assert(Remoting == RemotingTool.ApServ);
                RunCommand("start apserv.exe " + machineName + " \"" + cmd + "\"", verbose);
            }
        }

        public static void RunCommand(string cmd, bool verbose = false)
        {
            if (verbose)
            {
                Console.WriteLine("CMD: " + cmd + "");
            }

            try
            {
                if (!IsFaked)
                {
                    var r = SystemHelper.RunCommand(cmd);
                    if (verbose)
                    {
                        Console.WriteLine(r);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception for " + cmd);
                Console.WriteLine(" Content: " + e.Message);
            }
        }
    }
}
