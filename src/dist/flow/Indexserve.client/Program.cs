using System;
using System.IO;

using rDSN.Tron.Utility;
using rDSN.Tron.App;

namespace Indexserve.client
{
    class Program
    {
        static void Main(string[] args)
        {
            IndexServe_Client client;
            if (args.Length > 0)
            {
                client = new IndexServe_Client(args[0]);
            }
            else
            { 
                 client = new IndexServe_Client("http://cosmos01/IS");
            }
            
            while (true)
            {
                for (int i = 0; i < 10; i++)
                {
                    StringQuery req = new StringQuery() { Query = "WebAnswer.FIFA 2014." + RandomGenerator.Random64()};
                    var begin = DateTime.Now;
                    var result = client.WebAnswer(req);
                    Console.WriteLine("Query = '" + req.Query + "', " + result.Results.Count + " results returned, finished in " + (DateTime.Now - begin).TotalSeconds + " seconds");
                }

                for (int i = 0; i < 10; i++)
                {
                    StringQuery req = new StringQuery() { Query = "Search1.FIFA 2014." + RandomGenerator.Random64() };
                    var begin = DateTime.Now;
                    var result = client.Search1(req);
                    Console.WriteLine("Query = '" + req.Query + "', " + result.Results.Count + " results returned, finished in " + (DateTime.Now - begin).TotalSeconds + " seconds");
                }

                for (int i = 0; i < 10; i++)
                {
                    StringQuery req = new StringQuery() { Query = "Search2.FIFA 2014." + RandomGenerator.Random64() };
                    var begin = DateTime.Now;
                    var result = client.Search2(req);
                    Console.WriteLine("Query = '" + req.Query + "', " + result.Results.Count + " results returned, finished in " + (DateTime.Now - begin).TotalSeconds + " seconds");
                }
            }
        }
    }
}
