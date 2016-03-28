using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.example 
{
    // server app example
    public class counterServerApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            _counterServer.OpenService();
            return ErrorCode.ERR_OK;
        }

        public override void Stop(bool cleanup = false)
        {
            _counterServer.CloseService();
            _counterServer.Dispose();
        }

        private counterServerImpl _counterServer = new counterServerImpl();
    }

    // client app example
    public class counterClientApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            if (args.Length < 2)
            {
                throw new Exception("wrong usage: server-url or server-host server-port");                
            }

            if (args.Length == 2)
            {
                _server = new RpcAddress(args[1]);
            }
            else
            {
                _server = new RpcAddress(args[1], ushort.Parse(args[2]));
            }

            _counterClient = new counterClient(_server);
            _timer = Clientlet.CallAsync2(counterHelper.LPC_COUNTER_TEST_TIMER, null, this.OnTestTimer, 0, 0, 1000);
            return ErrorCode.ERR_OK;
        }

        public override void Stop(bool cleanup = false)
        {
            _timer.Cancel(true);
            _counterClient.Dispose();
            _counterClient = null;
        }

        private void OnTestTimer()
        {
            // test for service 'counter'
            {
                count_op req = new count_op();
                req.Name = testHelper.add_name;
                req.Operand = testHelper.add_operand;
                //sync:
                count_result resp;
                var err = _counterClient.add(req, out resp);
                if (err == ErrorCode.ERR_OK)
                {
                    bool ok = resp.Value == testHelper.add_ret;
                    testHelper.add_result("client test add", ok);
                }
                //async: 
                // TODO:
           
            }
            {
                count_name req = new count_name();
                req.Name = testHelper.read_name;
                //sync:
                count_result resp;
                var err = _counterClient.read(req, out resp);
                if (err == ErrorCode.ERR_OK)
                {
                    bool ok = resp.Value == testHelper.read_ret;
                    testHelper.add_result("client test read", ok);
                }
                //async: 
                // TODO:

            }
        }

        private SafeTaskHandle _timer;
        private RpcAddress  _server = new RpcAddress();
        
        private counterClient _counterClient;
    }
} // end namespace
