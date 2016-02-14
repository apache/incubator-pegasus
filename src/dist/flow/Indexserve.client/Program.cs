/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */
 
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
