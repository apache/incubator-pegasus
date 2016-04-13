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
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in rDSN.CSharp project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections.Generic;

namespace dsn.dev.csharp
{
    using dsn_message_t = IntPtr;
    using dsn_task_t = IntPtr;
    
    public class RpcReplier<TResponse>
    {
        public delegate void Marshaller(RpcWriteStream respStream, TResponse resp);
        public RpcReplier(RpcWriteStream respStream, Marshaller marshal)
        {
            _respStream = respStream;
            _marshaller = marshal;
        }

        public void Reply(TResponse resp)
        {
            _marshaller(_respStream, resp);

            Logging.dassert(_respStream.IsFlushed(),
                "RpcWriteStream must be flushed after write in the same thread");

            Native.dsn_rpc_reply(_respStream.DangerousGetHandle());
        }

        private RpcWriteStream _respStream;
        private Marshaller _marshaller;
    }

    public class Serverlet<T> : Clientlet
        where T : Serverlet<T>
    {
        public Serverlet(string name, int task_bucket_count = 13)
            : base(task_bucket_count)
        {
            _name = name;
            _handlers = new Dictionary<TaskCode, dsn_rpc_request_handler_t>();
        }

        ~Serverlet()
        {
            foreach (var c in _handlers)
            {
                UnregisterRpcHandler(c.Key);
            }   
        }

        protected delegate void RpcRequestHandlerOneWay(RpcReadStream req);

        protected delegate void RpcRequestHandler(RpcReadStream req, RpcWriteStream resp);

        protected bool RegisterRpcHandler(TaskCode code, string name, RpcRequestHandlerOneWay handler)
        {
            dsn_rpc_request_handler_t cb = (req, param) =>
                {
                    var rms = new RpcReadStream(req, false);
                    handler(rms);
                };

            var r = Native.dsn_rpc_register_handler(code, name, cb, IntPtr.Zero, IntPtr.Zero);
            Logging.dassert(r, "rpc handler registration failed for " + code);

            lock (_handlers)
            {
                _handlers.Add(code, cb);
            }
            return r;
        }

        protected bool RegisterRpcHandler(TaskCode code, string name, RpcRequestHandler handler)
        {
            dsn_rpc_request_handler_t cb = (req, param) =>
            {
                // if handler synchnously processes the incoming request
                // we don't need to add_ref and set owner to true 
                // in folloiwng two stmts
                // however, we don't know so we do as follows
                Native.dsn_msg_add_ref(req); // released by RpcReadStream 
                var rms = new RpcReadStream(req, true);

                var wms = new RpcWriteStream(Native.dsn_msg_create_response(req));
                handler(rms, wms);    
            };

            var r = Native.dsn_rpc_register_handler(code, name, cb, IntPtr.Zero, IntPtr.Zero);
            Logging.dassert(r, "rpc handler registration failed for " + code);

            lock (_handlers)
            {
                _handlers.Add(code, cb);
            }
            return true;
        }

        protected bool UnregisterRpcHandler(TaskCode code)
        {
            Native.dsn_rpc_unregiser_handler(code, IntPtr.Zero);
            bool r;

            lock (_handlers)
            {
                r = _handlers.Remove(code); 
            }
            return r;
        }

        protected void Reply(RpcWriteStream response)
        {
            Logging.dassert(response.IsFlushed(),
                "RpcWriteStream must be flushed after write in the same thread");

            Native.dsn_rpc_reply(response.DangerousGetHandle());
        }

        public string Name() { return _name; }

        private string _name;
        private Dictionary<TaskCode, dsn_rpc_request_handler_t> _handlers;
    }
}
