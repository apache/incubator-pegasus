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
    
    public class Serverlet<T> : Servicelet
        where T : Serverlet<T>
    {
        public Serverlet(string name, int task_bucket_count = 13)
            : base(task_bucket_count)
        {
            _name = name;
            _codes = new HashSet<TaskCode>();
        }

        ~Serverlet()
        {
            foreach (var c in _codes)
            {
                UnregisterRpcHandler(c);
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

            lock (_codes)
            {
                _codes.Add(code);
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
                wms.Flush();
            };

            bool r = Native.dsn_rpc_register_handler(code, name, cb, IntPtr.Zero);
            Logging.dassert(r, "rpc handler registration failed for " + code.ToString());

            lock(_codes)
            {
                _codes.Add(code);
            }
            return true;
        }

        protected bool UnregisterRpcHandler(TaskCode code)
        {
            Native.dsn_rpc_unregiser_handler(code);
            bool r;

            lock(_codes)
            {
                r = _codes.Remove(code); 
            }
            return r;
        }

        public string Name() { return _name; }

        private string _name;
        private HashSet<TaskCode> _codes;
    }
}
