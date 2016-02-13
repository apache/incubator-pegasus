using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;

using rDSN.Tron.Utility;

namespace rDSN.Tron.ControlPanel
{
    public class MetaServerController : Command
    {
        public override bool Execute(List<string> args)
        {
            if (args.Count < 2)
                return false;

            // return "[m|M]eta machine deploy|start|stop|clear [verbose = false] [faked = false] [dir = d:\\" + System.Environment.UserName + "\\Tron\\MetaServer] \n";
            string machine = args[0];
            string command = args[1];

            bool verbose = false;
            if (args.Count > 2)
                verbose = bool.Parse(args[2]);

            bool faked = false;
            if (args.Count > 3)
                faked = bool.Parse(args[3]);

            string dir = "d:\\" + System.Environment.UserName + "\\Tron\\MetaServer";
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
            "MetaServer.exe",
            "ServiceStore.exe",
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
            DeployHelper.EnableFirewallRule("TronMetaServer", dir + "\\" + "ServiceStore.exe", machineName, verbose);
        }

        private void Start(string machineName, string dir, bool verbose)
        {
            DeployHelper.RunRemoteCommand(machineName, "cmd.exe /c START ServiceStore.exe", verbose, dir);
        }

        private void Stop(string machineName, bool verbose)
        {
            string cmd = @"TASKKILL /S " + machineName + " /F /IM ServiceStore.exe";
            DeployHelper.RunCommand(cmd, verbose);

            cmd = @"TASKKILL /S " + machineName + " /F /IM MetaServer.exe";
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
            return "[m|M]eta machine deploy|start|stop|clear [verbose = false] [faked = false] [dir = d:\\" + System.Environment.UserName + "\\Tron\\MetaServer]";
        }

        public override string Usage() { return Help(); }
    }
}
