using System;
using dsn.dev.csharp;

namespace dsn.example 
{
    // server app example
    public class echoServerApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            _echoServer.OpenService(Gpid());
            return ErrorCode.ERR_OK;
        }

        public override ErrorCode Stop(bool cleanup = false)
        {
            _echoServer.CloseService(Gpid());
            _echoServer.Dispose();

            return ErrorCode.ERR_OK;
        }

        private echoServer _echoServer = new echoServer();
    }

    // client app example
    public class echoClientApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            if (args.Length < 2)
            {
                throw new Exception("wrong usage: server-host server-port or service-url");                
            }

            if (args.Length >= 3)
            {
                _server.addr = Native.dsn_address_build(args[1], ushort.Parse(args[2]));
            }
            else
            {
                if (args[1].Contains("dsn://"))
                    _server = new RpcAddress(args[1]);
                else
                {
                    var addrs = args[1].Split(new char[] { ':'}, StringSplitOptions.RemoveEmptyEntries);
                    _server.addr = Native.dsn_address_build(addrs[0], ushort.Parse(addrs[1]));
                }
            }

            _echoClient = new echoClient(_server);
            _timer = Clientlet.CallAsync2(echoHelper.LPC_ECHO_TEST_TIMER, null, this.OnTestTimer, 0, 0, 1000);
            return ErrorCode.ERR_OK;
        }

        public override ErrorCode Stop(bool cleanup = false)
        {
            _timer.Cancel(true);
            _echoClient.Dispose();
            _echoClient = null;

            return ErrorCode.ERR_OK;
        }

        private void OnTestTimer()
        {
            // test for service 'echo'
            {
                var req = "PING";
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
