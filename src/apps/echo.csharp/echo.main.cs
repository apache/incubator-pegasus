using System;
using System.IO;
using System.Linq;
using dsn.dev.csharp;

namespace dsn.example 
{
    class Program
    {
        static void Main(string[] args)
        {
            echoHelper.InitCodes();

            ServiceApp.RegisterApp<echoServerApp>("server");
            ServiceApp.RegisterApp<echoClientApp>("client");        
            // ServiceApp.RegisterApp<echoPerfTestClientApp>("client.echo.perf.test");

            string[] args2 = (new string[] { "echo" }).Union(args).ToArray();
            Native.dsn_run(args2.Length, args2, true);
        }
    }
}
