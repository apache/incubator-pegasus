using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;

namespace rDSN.Tron.ControlPanel
{
    public class ServiceCreateCommand : Command
    {
        public ServiceCreateCommand()
        {
            _transport = new NetlibTransport(new BinaryProtocolFactory());
            InitProxy();
        }

        public override bool Execute(List<string> args)
        {
            if (args.Count < 2)
            {
                Console.WriteLine(Usage());
                return false;
            }

            ServiceInfo si = new ServiceInfo();
            si.InternalServiceSequenceId = 0;
            si.Name = args[1];
            si.PartitionCount = 1;
            si.ServicePackageName = args[0];


            for (int ik = 0; ik < 2; ik++)
            {
                try
                {
                    RpcError err2 = _proxy.CreateService(si);
                    Trace.WriteLine("Create service '" + si.Name + "', package = '" + si.ServicePackageName + "', error = " + ((ErrorCode)err2.Code).ToString());
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

        public override string Help()
        {
            return "CreateService|CS|cs packageName serviceName";
        }

        public override string Usage()
        {
            return Help();
        }

        private ITransport _transport;
        private MetaServer_Proxy _proxy;
    }
}
