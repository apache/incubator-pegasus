using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.example 
{
    public class echoClient : Clientlet
    {
        private RpcAddress _server;
        
        public echoClient(RpcAddress server) { _server = server; }
        public echoClient() { }
        ~echoClient() {}

    
        // ---------- call echoHelper.RPC_ECHO_ECHO_PING ------------
        // - synchronous 
        public ErrorCode ping(
            string val, 
            out string resp, 
            int timeout_milliseconds = 0, 
            int hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(echoHelper.RPC_ECHO_ECHO_PING, timeout_milliseconds, hash);
            s.Write(val);
            s.Flush();
            
            var respStream = RpcCallSync(server != null ? server : _server, s);
            if (null == respStream)
            {
                resp = default(string);
                return ErrorCode.ERR_TIMEOUT;
            }
            else
            {
                respStream.Read(out resp);
                return ErrorCode.ERR_OK;
            }
        }
        
        // - asynchronous with on-stack string and string 
        public delegate void pingCallback(ErrorCode err, string resp);
        public void ping(
            string val, 
            pingCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(echoHelper.RPC_ECHO_ECHO_PING,timeout_milliseconds, request_hash);
            s.Write(val);
            s.Flush();
            
            RpcCallAsync(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                string resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }        
        
        public SafeTaskHandle ping2(
            string val, 
            pingCallback callback,
            int timeout_milliseconds = 0, 
            int reply_hash = 0,
            int request_hash = 0,
            RpcAddress server = null)
        {
            RpcWriteStream s = new RpcWriteStream(echoHelper.RPC_ECHO_ECHO_PING,timeout_milliseconds, request_hash);
            s.Write(val);
            s.Flush();
            
            return RpcCallAsync2(
                        server != null ? server : _server, 
                        s,
                        this, 
                        (err, rs) => 
                            { 
                                string resp;
                                rs.Read(out resp);
                                callback(err, resp);
                            },
                        reply_hash
                        );
        }       
    
    }

} // end namespace
