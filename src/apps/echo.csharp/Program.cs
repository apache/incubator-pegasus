using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using dsn.dev.csharp;
using System.IO;

namespace echo.csharp
{
    public class EchoClientApp : ServiceApp
    {
        public static TaskCode LPC_ECHO_TIMER1;
        public static TaskCode LPC_ECHO_TIMER2;
        public static TaskCode RPC_ECHO;

        public static void InitCodes()
        {
            LPC_ECHO_TIMER1 = new TaskCode("LPC_ECHO_TIMER1", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            LPC_ECHO_TIMER2 = new TaskCode("LPC_ECHO_TIMER2", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            RPC_ECHO = new TaskCode("RPC_ECHO", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
        }

        public override ErrorCode Start(string[] args)
        {
            _client = new EchoServiceClient();
            _client.Start(args);
            return new ErrorCode(0);
        }

        public override void Stop(bool cleanup = false)
        {
            _client.Dispose();
        }

        private EchoServiceClient _client;
    }

    public class EchoServerApp : ServiceApp
    {
        public override ErrorCode Start(string[] args)
        {
            _server = new EchoServiceServer();
            _server.OpenService();
            return new ErrorCode(0);
        }

        public override void Stop(bool cleanup = false)
        {
            _server.CloseService();
            _server.Dispose();
        }

        private EchoServiceServer _server;
    }

    public static class RpcStreamHelper
    {
        public static void Read(this RpcReadStream s, out string v)
        {
            StreamReader reader = new StreamReader(s);
            v = reader.ReadToEnd();
        }
        
        public static void Write(this RpcWriteStream s, string v)
        {
            var bytes = Encoding.UTF8.GetBytes(v);
            s.Write(bytes, 0, bytes.Length);
        }
    }
    
    public class EchoServiceServer : Serverlet<EchoServiceServer>
    {
        public EchoServiceServer()
            : base("echo.server")
        {

        }

        public void OnEcho(RpcReadStream request, RpcWriteStream response)
        {
            string v;
            request.Read(out v);
            response.Write(v);
            response.Flush();
            Reply(response);
        }

        public void OpenService()
        {
            RegisterRpcHandler(EchoClientApp.RPC_ECHO, "Echo", this.OnEcho);
        }

        public void CloseService()
        {
            UnregisterRpcHandler(EchoClientApp.RPC_ECHO);
        }
    }

    public class EchoServiceClient : Servicelet
    {
        public void Start(string[] argv)
        {
            _last_ts = DateTime.Now;

            if (argv.Length < 3)
            {
                throw new Exception("wrong usage: EchoServiceClient server-host server-port");                
            }

            _server = new RpcAddress();
            Native.dsn_address_build(out _server.addr, argv[1], ushort.Parse(argv[2]));

            //CallAsync(EchoClientApp.LPC_ECHO_TIMER1, this,  this.OnTimer1, 0, 0);
            CallAsync(EchoClientApp.LPC_ECHO_TIMER2, this, () => this.OnTimer2(100), 0, 0);
        }

        public void OnTimer1()
        {
            //Console.WriteLine("on_timer1");
            CallAsync(EchoClientApp.LPC_ECHO_TIMER1, this, this.OnTimer1, 0, 0);

            var c = ++_count_timer1;
            if (c % 10000 == 0)
            {
                var gap = DateTime.Now - _last_ts;
                _last_ts = DateTime.Now;

                Console.WriteLine("Timer1: Cost {0} ms for {1} tasks", gap.TotalMilliseconds, _count_timer1);
                _count_timer1 = 0;

                //cancel_all_pending_tasks();
            }
        }

        public void OnTimer2(object param)
        {
            //Console.WriteLine("on_timer2 " + param.ToString());
            RpcWriteStream s = new RpcWriteStream(EchoClientApp.RPC_ECHO, 1000, 0);
            s.Write("hi, this is timer2 echo");
            s.Flush();
            RpcCallAsync(_server, s, this, this.OnTimer2EchoCallback, 0);
        }

        public void OnTimer2EchoCallback(ErrorCode err, RpcReadStream response)
        {
            if (err == ErrorCode.ERR_OK)
            {
                string v;
                response.Read(out v);
                //Logging.dassert(v == "hi, this is timer2 echo",
                //"incorrect responsed value");
            }

            var c = ++_count_timer2;
            if (c % 10000 == 0)
            {
                var gap = DateTime.Now - _last_ts;
                _last_ts = DateTime.Now;

                Console.WriteLine("Timer2: Cost {0} ms for {1} tasks", gap.TotalMilliseconds, _count_timer2);
                _count_timer1 = 0;

                //cancel_all_pending_tasks();
            }

            string cs = new string('v', 1024 * 1024);
            RpcWriteStream s = new RpcWriteStream(EchoClientApp.RPC_ECHO, 1000, 0);
            s.Write(cs);
            s.Flush();
            RpcCallAsync(_server, s, this, this.OnTimer2EchoCallback, 0);
        }

        private RpcAddress _server;
        private int _count_timer1 = 0;
        private int _count_timer2 = 0;
        private DateTime _last_ts;
    }

    class Program
    {
        static void Main(string[] args)
        {
            EchoClientApp.InitCodes();

            ServiceApp.RegisterApp<EchoClientApp>("client");
            ServiceApp.RegisterApp<EchoServerApp>("server");

            string[] args2 = (new string[] { "echo.exe" }).Union(args).ToArray();
            Native.dsn_run(args2.Length, args2, true);
        }
    }
}
