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

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron;

namespace rDSN.Tron.ControlPanel
{
    public class QueryServiceCommand : Command
    {
        public QueryServiceCommand()
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
                    var resp = _proxy.QueryServices(req);

                    ErrorCode c = (ErrorCode)resp.Code;
                    Console.WriteLine("query error code: " + c.ToString());

                    if (c == ErrorCode.Success)
                    {
                        if (args.Count > 0)
                        {
                            for (int i = 0; i < args.Count; i++)
                            {
                                var rresp = resp.Value.Services[i];
                                Console.WriteLine("  " + args[i] + ", query code:" + (ErrorCode)rresp.Code);
                                if (rresp.Code == 0)
                                {
                                    rresp.Value.Echo("service", 1);
                                }
                            }
                        }
                        else
                        {
                            foreach (var m in resp.Value.Services)
                            {
                                m.Value.Echo("service", 1);
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
                
        public override string Help()
        {
            return "QueryService|qs|QS servicename1 servicename2 ...";
        }

        public override string Usage()
        {
            return Help();
        }

        private ITransport _transport;
        private MetaServer_Proxy _proxy;
    }
}
