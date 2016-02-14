using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.example 
{
    // server app example
    public class echoServerApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            _echoServer.OpenService();
            return ErrorCode.ERR_OK;
        }

        public override void Stop(bool cleanup = false)
        {
            _echoServer.CloseService();
            _echoServer.Dispose();
        }

        private echoServer _echoServer = new echoServer();
    }

    // client app example
    public class echoClientApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            if (args.Length < 3)
            {
                throw new Exception("wrong usage: server-host server-port");                
            }

            _server.addr = Native.dsn_address_build(args[1], ushort.Parse(args[2]));

            _echoClient = new echoClient(_server);
            _timer = Clientlet.CallAsync2(echoHelper.LPC_ECHO_TEST_TIMER, null, this.OnTestTimer, 0, 0, 1000);
            return ErrorCode.ERR_OK;
        }

        public override void Stop(bool cleanup = false)
        {
            _timer.Cancel(true);
            _echoClient.Dispose();
            _echoClient = null;
        }

        private void OnTestTimer()
        {
            // test for service 'echo'
            {
                string req = "PING";
                //sync:
                string resp;
                var err = _echoClient.ping(req, out resp);
                Console.WriteLine("call RPC_ECHO_ECHO_PING end, return " + err.ToString());
                //async: 
                // TODO:
           
            }
        }

        private SafeTaskHandle _timer;
        private RpcAddress  _server = new RpcAddress();
        
        private echoClient _echoClient;
    }

    /*
    class echo_perf_testClientApp :
        public ::dsn::service_app<echo_perf_testClientApp>, 
        public virtual ::dsn::service::clientlet
    {
    public:
        echo_perf_testClientApp()
        {
            _echoClient= null;
        }

        ~echo_perf_testClientApp()
        {
            stop();
        }

        virtual ErrorCode start(int argc, char** argv)
        {
            if (argc < 2)
                return ErrorCode.ERR_INVALID_PARAMETERS;

            dsn_address_build(_server.c_addr_ptr(), argv[1], (uint16_t)atoi(argv[2]));

            _echoClient= new echo_perf_testClient(_server);
            _echoClient->start_test();
            return ErrorCode.ERR_OK;
        }

        virtual void stop(bool cleanup = false)
        {
            if (_echoClient!= null)
            {
                delete _echoClient;
                _echoClient= null;
            }
        }
        
    private:
        echo_perf_testClient*_echoClient;
        RpcAddress _server;
    }
    */
} // end namespace
