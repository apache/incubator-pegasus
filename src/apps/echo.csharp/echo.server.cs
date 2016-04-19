using System;
using dsn.dev.csharp;

namespace dsn.example  
{
    public class echoServer : Serverlet<echoServer>
    {
        public echoServer() : base("echo") {}
        ~echoServer() { }
    
        // all service handlers to be implemented further
        // RPC_ECHO_ECHO_PING 
        private void OnpingInternal(RpcReadStream request, RpcWriteStream response)
        {
            string val;
            
            try 
            {
                request.Read(out val);
            } 
            catch (Exception)
            {
                // TODO: error handling
                return;
            }

            Onping(val, new RpcReplier<string>(response, (s, r) => 
            { 
                s.Write(r);
                s.Flush();
            }));
        }
        
        protected virtual void Onping(string val, RpcReplier<string> replier)
        {
            Console.WriteLine("... exec RPC_ECHO_ECHO_PING ... (not implemented) ");
            var resp = val;
            replier.Reply(resp);
        }
        
        
        public void OpenService(UInt64 gpid)
        {
            RegisterRpcHandler(echoHelper.RPC_ECHO_ECHO_PING, "ping", this.OnpingInternal, gpid);
        }

        public void CloseService(UInt64 gpid)
        {
            UnregisterRpcHandler(echoHelper.RPC_ECHO_ECHO_PING, gpid);
        }
    }


} // end namespace
