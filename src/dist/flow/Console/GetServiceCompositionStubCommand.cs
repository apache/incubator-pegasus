using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron.Runtime;

namespace rDSN.Tron.ControlPanel
{
    public class GetServiceCompositionStubCommand : Command
    {
        public GetServiceCompositionStubCommand()
        {
            _transport = new NetlibTransport(new BinaryProtocolFactory());
            _saveDir = Configuration.Instance().Get<string>("ServicePackageSaveDir", Path.Combine(Directory.GetCurrentDirectory(), "ServicePackage"));
            if (!Directory.Exists(_saveDir))
            {
                Directory.CreateDirectory(_saveDir);
            }

            InitProxy();
        }

        public override bool Execute(List<string> args)
        {
            var req = new Name();
            if (args.Count == 0)
            {
                Console.WriteLine(Usage());
                return false;
            }

            try
            {
                req.Value = args[0];
                var resp = _proxy.GetServiceCompositionAssembly(req);

                ErrorCode c = (ErrorCode)resp.Code;
                Console.WriteLine("query error code: " + c.ToString());

                if (c == ErrorCode.Success)
                {
                    var sc = resp.Value;

                    SystemHelper.ByteArrayToFile(
                        sc.Data.Array,
                        sc.Data.Offset,
                        sc.Data.Count,
                        Path.Combine(_saveDir, args[0] + ".Tron.Composition.dll")
                        );

                    Console.WriteLine("Service implementation downloaded to " + Path.Combine(_saveDir, args[0] + ".Tron.Composition.dll"));
                }

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("exception, msg = " + e.Message);
                InitProxy();
            }

            return false;
        }

        private void InitProxy()
        {
            NodeAddress storeAddress = new NodeAddress();
            storeAddress.Host = Configuration.Instance().Get<string>("ServiceStore", "Host", "localhost");
            storeAddress.Port = Configuration.Instance().Get<int>("ServiceStore", "Port", 19995);

            _proxy = new ServiceStore_Proxy(_transport.Connect(storeAddress.Host, storeAddress.Port));
        }
        
        public override string Help()
        {
            return "GetCompositionStub|GC|gc ServicePackageName ...";
        }

        public override string Usage()
        {
            return Help();
        }

        private ITransport _transport;
        private ServiceStore_Proxy _proxy;
        private string _saveDir;
    }
}
