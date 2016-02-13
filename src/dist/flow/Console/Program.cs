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
            Configuration.Instance().Set("VerboseUDP", "false");
            Configuration.Instance().Set("VerboseRpc", "false");

            ServiceNode.Instance().Start();
                        
            // general commands
            CommandManager.Instance().Add(new string[] {"quit", "Quit", "q", "Q"}, new ExitCommand());
            CommandManager.Instance().Add(new string[] { "help", "Help", "h", "H" }, new HelpCommand());            
            CommandManager.Instance().Add(new string[] { "Repeat", "repeat", "r", "R" }, new RepeatCommand());            
            CommandManager.Instance().Add(new string[] { "enum", "Enum", "e", "E" }, new EnumCommand());
            CommandManager.Instance().Add(new string[] { "win", "Win", "w", "W" }, new WinBatchCommand());

            // service infrastructure commands
            CommandManager.Instance().Add(new string[] { "QueryMachine", "QM", "qm" }, new QueryMachineCommand());
            CommandManager.Instance().Add(new string[] { "QueryService", "QS", "qs" }, new QueryServiceCommand());
            CommandManager.Instance().Add(new string[] { "server", "Server", "s", "S" }, new AppServerController());
            CommandManager.Instance().Add(new string[] { "meta", "Meta", "m", "M" }, new MetaServerController());
            CommandManager.Instance().Add(new string[] { "CreateService", "CS", "cs"}, new ServiceCreateCommand());
            CommandManager.Instance().Add(new string[] { "RemoveService", "RS", "rs" }, new ServiceRemoveCommand());

            // service store commands
            CommandManager.Instance().Add(new string[] { "PublishPackage", "PP", "pp" }, new PublishServicePackageCommand());
            CommandManager.Instance().Add(new string[] { "QueryPackage", "QP", "qp" }, new QueryServicePackageCommand());
            CommandManager.Instance().Add(new string[] { "GetPackage", "GP", "gp" }, new GetServicePackageCommand());
            CommandManager.Instance().Add(new string[] { "GetCompositionStub", "GC", "gc" }, new GetServiceCompositionStubCommand());
            CommandManager.Instance().Add(new string[] { "GetServiceClient", "GSC", "gsc" }, new GetServiceClientLibCommand());

            // local tools
            //CommandManager.Instance().Add(new string[] { "Generate", "generate", "g", "G" }, new GenerateCommand());
            //CommandManager.Instance().Add(new string[] { "Generatei", "generatei", "gi", "GI" }, new GenerateCompositionStubCommand());
            //CommandManager.Instance().Add(new string[] { "Generateb", "generateb", "gb", "GB" }, new GenerateIDLFileCommand());
            //CommandManager.Instance().Add(new string[] { "Generatec", "generatec", "gc", "GC" }, new GenerateCommonInterfaceCommand());

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
