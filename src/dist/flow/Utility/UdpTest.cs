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
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Collections;
using System.IO;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Linq;


namespace rDSN.Tron.Utility
{
    public class UdpTest
    {
        public static void Test(string[] args)
        {
            // client
            if (args.Length == 2)
            {
                var c = new UdpClient();
                c.Start(args[0], int.Parse(args[1]));
            }

            // server
            else if (args.Length == 1)
            {
                var c = new UdpServer();
                c.Start(int.Parse(args[0]));
            }

            else
            {
                Console.WriteLine("incorrect parameters.");
            }
        }
    }

    public class UdpServer
    {
        public void Start(int port)
        {
            Console.WriteLine("UPD Server on " + port + " ...");

            var sep = new System.Net.IPEndPoint(IPAddress.Any, port);
            _udp_socket = new Socket(sep.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
            _udp_socket.Bind(sep);

            while (true)
            {
                byte[] s = new byte[1024];
                EndPoint ep = new IPEndPoint(IPAddress.Any, 0);
                try
                {
                    int read = _udp_socket.ReceiveFrom(s, ref ep);
                    if (read > 0)
                    {
                        Console.WriteLine("UDP recv ok from " +  ep.ToString());
                    }
                }
                catch (SocketException e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }
        
        private static Socket _udp_socket = null;
    }

    public class UdpClient
    {
        public void Start(string server, int port)
        {
            UInt32 ip = 0;
            IPHostEntry ipHostInfo = Dns.GetHostEntry(server);
            for (int i = ipHostInfo.AddressList.Length - 1; i >= 0; i--)
            {
                if (ipHostInfo.AddressList[i].AddressFamily == AddressFamily.InterNetwork)
                {
                    IPAddress ipAddress = ipHostInfo.AddressList[i];
                    ip = BitConverter.ToUInt32(ipAddress.GetAddressBytes(), 0);
                    break;
                }
            }

            EndPoint ep = new IPEndPoint(ip, port);

            Console.WriteLine("UPD Client for " + ep.ToString() + "...");

            while (true)
            {
                SendMsg(ep);
                Thread.Sleep(1000);
            }
        }

        private static int _i = 0;
        private static Socket _udp_sock = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        private static void MsgSentCallback(IAsyncResult ar)
        {
            Console.WriteLine("UDP send callback for msg " + (int)ar.AsyncState);
        }

        private static int SendMsg(EndPoint server)
        {
            byte[] s = new byte[] { 0, 1, 2, 3, 4 };
            _udp_sock.BeginSendTo(s, 0, (int)s.Length, 0, server, new AsyncCallback(MsgSentCallback), ++_i);
            //_udp_sock.SendTo(s, server);
            Console.WriteLine("UDP send begin for msg " + _i);
            return 0;
        }
    }
}
