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

        protected override void Onadd(count_op args, RpcReplier<count_result> replier)
        {
            bool ok = args.Name == testHelper.add_name && args.Operand == testHelper.add_operand;
            testHelper.add_result("server test on_add", ok);
            var resp = new count_result();
            resp.Value = testHelper.add_ret;
            replier.Reply(resp);
        }

        protected override void Onread(count_name args, RpcReplier<count_result> replier)
        {
            bool ok = args.Name == testHelper.read_name;
            testHelper.add_result("server test on_read", ok);
            var resp = new count_result();
            resp.Value = testHelper.read_ret;
            replier.Reply(resp);
        }
    }
}
