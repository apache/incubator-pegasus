using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

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

    public class Serverlet<T> : Servicelet
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
            dsn_rpc_request_handler_t cb = (dsn_message_t req, IntPtr param) =>
                {
                    RpcReadStream rms = new RpcReadStream(req, false);
                    handler(rms);
                };

            bool r = Native.dsn_rpc_register_handler(code, name, cb, IntPtr.Zero);
            Logging.dassert(r, "rpc handler registration failed for " + code.ToString());

            lock (_handlers)
            {
                _handlers.Add(code, cb);
            }
            return r;
        }

        protected bool RegisterRpcHandler(TaskCode code, string name, RpcRequestHandler handler)
        {
            dsn_rpc_request_handler_t cb = (dsn_message_t req, IntPtr param) =>
            {
                RpcReadStream rms = new RpcReadStream(req, false);
                RpcWriteStream wms = new RpcWriteStream(Native.dsn_msg_create_response(req), false);
                handler(rms, wms);    
            };

            bool r = Native.dsn_rpc_register_handler(code, name, cb, IntPtr.Zero);
            Logging.dassert(r, "rpc handler registration failed for " + code.ToString());

            lock (_handlers)
            {
                _handlers.Add(code, cb);
            }
            return true;
        }

        protected bool UnregisterRpcHandler(TaskCode code)
        {
            Native.dsn_rpc_unregiser_handler(code);
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
