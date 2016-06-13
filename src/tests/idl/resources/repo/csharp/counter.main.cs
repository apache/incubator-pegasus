using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using dsn.dev.csharp;

namespace dsn.example 
{
    class Program
    {
        static void Main(string[] args)
        {
            counterHelper.InitCodes();
            testHelper.init();

            ServiceApp.RegisterApp<counterServerApp>("server");
            ServiceApp.RegisterApp<counterClientApp>("client");        

            string[] args2 = (new string[] { "counter" }).Union(args).ToArray();
                Native.dsn_run(args2.Length, args2, false);
           // Native.dsn_run_config("config.ini", false);
            int counter = 0;
            while (testHelper.finished_test() != 4)
            {
                Thread.Sleep(1000);
                if (++counter > 10)
                {
                    Console.WriteLine("it takes too long to complete...");
                    Native.dsn_exit(1);
                }
            }
            bool passed = true;
            Dictionary<string, bool> result = testHelper.result();
            foreach (KeyValuePair<string, bool> entry in result)
            {
                string name = entry.Key;
                bool ok = entry.Value;
                passed = passed && ok;
                Console.WriteLine(name + " " + (ok ? "passed" : "failed"));
            }
            int ret = passed ? 0 : 1;
            Native.dsn_exit(ret);
        }
    }
}
