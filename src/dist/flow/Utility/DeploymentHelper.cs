using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        public static RemotingTool Remoting = Utility.RemotingTool.PsExec;

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

            string rpath = "\\\\" + remoteMachine + "\\" + remoteFullPath.Replace(':', '$');
            RunCommand("robocopy " + (cleanup ? "/MIR" : "") + " " + localFullPath + " " + rpath + (cleanup ? "" : " /S"), verbose);
        }

        public static void RemoteCopy(string localFullPath, string remoteMachine, string remoteFullPath, bool verbose = false)
        {
            localFullPath = localFullPath.PathClean();
            remoteFullPath = remoteFullPath.PathClean();

            string rpath = "\\\\" + remoteMachine + "\\" + remoteFullPath.Replace(':', '$');
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
            else if (Remoting ==  Utility.RemotingTool.PsExec)
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
                Trace.Assert(Remoting == Utility.RemotingTool.ApServ);
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
