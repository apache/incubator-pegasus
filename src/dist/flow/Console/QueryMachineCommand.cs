using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;

namespace rDSN.Tron.ControlPanel
{
    public class QueryMachineCommand : Command
    {
        public QueryMachineCommand()
        {
            _transport = new NetlibTransport(new BinaryProtocolFactory());
            InitProxy();
        }

        public override bool Execute(List<string> args)
        {
            var req = new NameList();
            foreach (var m in args)
            {
                req.Names.Add(m);
            }

            for (int ik = 0; ik < 2; ik++)
            {
                try
                {
                    var resp = _proxy.QueryServersByName(req);

                    ErrorCode c = (ErrorCode)resp.Code;
                    Console.WriteLine("query error code: " + c.ToString());

                    if (c == ErrorCode.Success)
                    {
                        if (args.Count > 0)
                        {
                            for (int i = 0; i < args.Count; i++)
                            {
                                var rresp = resp.Value.Servers[i];
                                Console.WriteLine("  " + args[i] + ", query code:" + (ErrorCode)rresp.Code);
                                if (rresp.Code == 0)
                                {
                                    foreach (var m in rresp.Value.Servers)
                                    {
                                        DumpServerInfo(m, "    ");
                                    }
                                }
                            }
                        }
                        else
                        {
                            foreach (var m in resp.Value.Servers.SelectMany(m1 => m1.Value.Servers))
                            {
                                DumpServerInfo(m, "  ");
                            }
                        }
                    }

                    return true;
                }
                catch (Exception e)
                {
                    Console.WriteLine("exception, msg = " + e.Message);
                    InitProxy();
                }
            }
            return false;
        }

        private void InitProxy()
        {
            NodeAddress metaAddress = new NodeAddress();
            metaAddress.Host = Configuration.Instance().Get<string>("MetaServer", "Host", "localhost");
            metaAddress.Port = Configuration.Instance().Get<int>("MetaServer", "Port", 20001);

            _proxy = new MetaServer_Proxy(_transport.Connect(metaAddress.Host, metaAddress.Port));
        }

        public static void DumpServerInfo(AppServerInfo server, string bs)
        {
            Console.WriteLine(bs + "Address: " + server.Host + ":" + server.Address.Port);
            Console.WriteLine(bs + "IsAlive: " + server.IsAlive.ToString());
            Console.WriteLine(bs + server.Services.Count + " services in total");
            foreach (var s in server.Services)
            {
                Console.WriteLine(bs + "  service " + s.Value.Name + "(ver = " + s.Value.ConfigurationVersion + ")" + " running on port " + s.Value.ServicePort);
            }
            Console.WriteLine(bs + "------");
        }

        public override string Help()
        {
            return "QueryMachine|qm|QM machinename1 machinename2 ...";
        }

        public override string Usage() 
        { 
            return Help(); 
        }

        private ITransport _transport;
        private MetaServer_Proxy _proxy;
    }
}
