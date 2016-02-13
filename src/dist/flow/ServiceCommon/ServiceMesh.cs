using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Reflection;
using System.Diagnostics;
using System.Threading;
using System.IO.Compression;

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.Runtime
{
    public abstract class ServiceMesh
    {
        public SLA Sla { get; private set; }
        
        private static ITransport _transport = new NetlibTransport(new BinaryProtocolFactory());
        private static NodeAddress _metaAddress = new NodeAddress();
        private static MetaServer_Proxy _meta = null;

        public ServiceMesh(SLA sla = null)
        {
            Sla = sla;

            if (null == _meta)
            {
                _metaAddress.Host = Configuration.Instance().Get<string>("MetaServer", "Host", "localhost");
                _metaAddress.Port = Configuration.Instance().Get<int>("MetaServer", "Port", 20001);
                _meta = new MetaServer_Proxy(_transport.Connect(_metaAddress.Host, _metaAddress.Port));
            }
        }

        public ErrorCode Start()
        {
            ErrorCode c = InitServicesAndClients();
            Thread.Sleep(500);
            return c;
        }

        protected abstract ErrorCode InitServicesAndClients();
                
        public static ServiceMesh Load(string dllFile, string name, SLA sla)
        {
            try
            {
                var asm = Assembly.LoadFrom(dllFile);
                var serviceTypes = asm.GetExportedTypes().Where(t => t.Name == "CsqlApplication").ToArray();
                Trace.Assert(serviceTypes.Length == 1);
                return (ServiceMesh)serviceTypes.First().GetConstructor(new Type[] { typeof(string), typeof(SLA) }).Invoke(new object[] { name, sla });
            }
            catch (Exception e)
            {
                Trace.WriteLine("Exception when loading service instance from '" + dllFile + "' with service type '" + "CsqlApplication" + "': " + e.Message);
                return null;
            }
        }

        protected ErrorCode InitService(string packageName, string url, string name)
        {
            ServiceInfo si = new ServiceInfo();
            si.InternalServiceSequenceId = 0;
            si.Name = name;
            si.PartitionCount = 1;
            si.ServicePackageName = packageName;

            RpcError err2 = _meta.CreateService(si);

            Trace.WriteLine("Init service '" + name + "', package = '" + packageName + "', error = " + ((ErrorCode)err2.Code).ToString());

            return (ErrorCode)(err2.Code);
        }

        protected ErrorCode InitClient(string name, out IBondTransportClient client)
        {
            NameList req = new NameList();
            req.Names.Add(name);

            ErrorCode code = ErrorCode.TimeOut;

            client = null;
            for (int i = 0; i < 50; i++)
            {
                try
                {
                    var result = _meta.QueryServices(req, new TimeSpan(0, 0, 1));
                    if (result.Code != (int)ErrorCode.Success)
                    {
                        code = (ErrorCode)result.Code;
                        break;
                    }
                    else
                    {
                        var si = result.Value.Services[0];
                        if (si.Code != (int)ErrorCode.Success)
                        {
                            code = (ErrorCode)si.Code;
                            break;
                        }

                        var ssi = si.Value;
                        if (ssi.Partitions[0].ServicePort == 0)
                        {
                            Thread.Sleep(200);
                            continue;
                        }

                        client = _transport.Connect(ssi.Partitions[0].ManagerAddress.Host, ssi.Partitions[0].ServicePort);
                        code = ErrorCode.Success;
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception during proxy init, msg = " + e.Message + ", stack trace = " + e.StackTrace);
                }
            }

            Trace.WriteLine("Init proxy for '" + name + "', error = " + code.ToString());

            return code;
        }
    }
}
