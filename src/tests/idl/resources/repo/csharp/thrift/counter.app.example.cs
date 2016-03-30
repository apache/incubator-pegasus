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
                counter.add_args req = new counter.add_args();
                req.Op = new count_op();
                req.Op.Name = testHelper.add_name;
                req.Op.Operand = testHelper.add_operand;
                //sync:
                counter.add_result resp;
                var err = _counterClient.add(req, out resp);
                if (err == ErrorCode.ERR_OK)
                {
                    bool ok = resp.Success == testHelper.add_ret;
                    testHelper.add_result("client test add", ok);
                }
                //async: 
                // TODO:
           
            }
            {
                counter.read_args req = new counter.read_args();
                req.Name = testHelper.read_name;
                //sync:
                counter.read_result resp;
                var err = _counterClient.read(req, out resp);
                if (err == ErrorCode.ERR_OK)
                {
                    bool ok = resp.Success == testHelper.read_ret;
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
