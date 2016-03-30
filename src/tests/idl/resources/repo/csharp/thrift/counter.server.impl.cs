using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using dsn.dev.csharp;

namespace dsn.example
{
    public class counterServerImpl : counterServer
    {

        protected override void Onadd(counter.add_args args, RpcReplier<counter.add_result> replier)
        {
            bool ok = args.Op.Name == testHelper.add_name && args.Op.Operand == testHelper.add_operand;
            testHelper.add_result("server test on_add", ok);
            var resp = new counter.add_result();
            resp.Success = testHelper.add_ret;
            replier.Reply(resp);
        }

        protected override void Onread(counter.read_args args, RpcReplier<counter.read_result> replier)
        {
            bool ok = args.Name == testHelper.read_name;
            testHelper.add_result("server test on_read", ok);
            var resp = new counter.read_result();
            resp.Success = testHelper.read_ret;
            replier.Reply(resp);
        }
    }
}
