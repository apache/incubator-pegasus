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
