using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.example  
{
    public class echoServer : Serverlet<echoServer>
    {
        public echoServer() : base("echo") {}
        ~echoServer() { CloseService(); }
    
        // all service handlers to be implemented further
        // RPC_ECHO_ECHO_PING 
        private void OnpingInternal(RpcReadStream request, RpcWriteStream response)
        {
            string val;
            
            try 
            {
                request.Read(out val);
            } 
            catch (Exception e)
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
        
        
        public void OpenService()
        {
            RegisterRpcHandler(echoHelper.RPC_ECHO_ECHO_PING, "ping", this.OnpingInternal);
        }

        public void CloseService()
        {
            UnregisterRpcHandler(echoHelper.RPC_ECHO_ECHO_PING);
        }
    }


} // end namespace
