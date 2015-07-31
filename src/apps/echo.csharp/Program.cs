using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using dsn.dev.csharp;

namespace echo.csharp
{
    public class echo_service : service_app
    {
        public override error_code start(string[] args)
        {
            return new error_code(0);
        }

        public override void stop(bool cleanup = false)
        {

        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            echo_service.register_app<echo_service>("echo");

            core.dsn_run_config("config.ini", true);
        }
    }
}
