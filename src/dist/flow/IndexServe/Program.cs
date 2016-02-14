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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Reflection;
using System.Diagnostics;
using System.Threading;

using BondNetlibTransport;
using BondTransport;

using rDSN.Tron.Utility;
using rDSN.Tron.Compiler;
using rDSN.Tron.Contract;

namespace rDSN.Tron.App
{
    public class IndexServe
    {
        private Service_IFEX sFEX = new Service_IFEX("http://cosmos01/FEX");
        private Service_IsCache sIsCache = new Service_IsCache("http://cosmos01/IsCache");
        private Service_QU sQU = new Service_QU("http://cosmos01/QU");
        private Service_QU2 sQU2 = new Service_QU2("http://cosmos01/QU2");
        private Service_WebCache sWebCache = new Service_WebCache("http://cosmos01/WebCache");
        private Service_SaaS sSaaS = new Service_SaaS("http://cosmos01/SaaS");
        private Service_RaaS sRaaS = new Service_RaaS("http://cosmos01/RaaS");
        private Service_CDG sCDG = new Service_CDG("http://cosmos01/CDG");

        public ISymbol<QueryResult> WebAnswer(ISymbol<StringQuery> keyword)
        {
            ISymbol<AugmentedQuery> augmentedQuery = null;

            return keyword
                .Call(q => sQU2.OnQueryAnnotation(q)) // query augmentation
                .Assign(out augmentedQuery)
                .Call(q => sWebCache.Get(q)) // check for web cache, using augmented query as key
                .IfThenElse(
                    r => r.Results.Count > 0,
                    hitPart => hitPart,
                    missPart2 => missPart2
                        .Call(q => sSaaS.OnL1Selection(q.Query))
                        .Scatter(sr => sr.Results) // for each PerDocStaticRank
                        .Gather(sr => sr.Top(s => s.StaticRank, 1000, false))
                        .Scatter() // for each PerDocStaticRank
                        .Call(r => new PerDocRank { Id = r.Pos.DocIdentity, Rank = sRaaS.OnL2Rank(r) })
                        .Gather(rs => rs.Top(r => r.Rank.Value, 100, false))
                        .Scatter() // for each PerDocRank
                        .Call(r => sCDG.Get(r.Id))
                        .Gather(cs => new QueryResult
                        {
                            Query = augmentedQuery.Value(),
                            Results = cs.ToList()
                        })
                        .AsyncCall(ar => sWebCache.Put(ar))
                    )
                ;
        }

        public ISymbol<QueryResult> Search2(ISymbol<StringQuery> query)
        {
            return query
                .Call(q => sIsCache.Get(q)) // check for IS cache first, using raw query as key
                .IfThenElse(
                    r => r.Results.Count > 0,
                    hit => hit,
                    miss => miss
                       .Call(q => sQU.OnQueryUnderstanding(q.RawQuery)) // query alterations
                       .Scatter(aqs => aqs.Alterations)
                       .CallEx(aq => WebAnswer(aq))
                       .Gather(// merge results from all alterations
                            rs => new QueryResult
                            {
                                RawQuery = query.Value(),
                                Results = rs.SelectMany(r => r.Results).ToList()
                            }
                       )

                )
                //.Call(r => new QueryResult2() { Keyword = query.Value().Query, Result = r })
                ;
        }

        public ISymbol<QueryResult> Search1(ISymbol<StringQuery> query)
        {
            ISymbol<AugmentedQuery> tempAq;
            ISymbol<QueryResult> qr;

            return query
                .Call(q => sIsCache.Get(q)) // check for IS cache first, using raw query as key
                .Assign(out qr)
                .IfThenElse(
                    r => r.Results.Count > 0,
                    hit => hit,
                    miss => miss
                       .Call(q => sQU.OnQueryUnderstanding(q.RawQuery)) // query alterations
                       .Scatter(aqs => aqs.Alterations)
                       .CallEx(aq => aq.Call(q => sQU2.OnQueryAnnotation(q)) // query annotation
                                    .Assign(out tempAq)
                                    .Call(q => sWebCache.Get(q)) // check for web cache, using augmented query as key
                                    .IfThenElse(
                                        r => r.Results.Count > 0,
                                        hitPart => hitPart,
                                        missPart2 => missPart2
                                            .Call(q => sSaaS.OnL1Selection(q.Query))
                                            .Scatter(rr => rr.Results)
                                            .Gather(ss => ss.Top(s => s.StaticRank, 1000, false))
                                            .Scatter()
                                            .Call(r => new PerDocRank { Id = r.Pos.DocIdentity, Rank = sRaaS.OnL2Rank(r) })
                                            .Gather(ss => ss.Top(r => r.Rank.Value, 100, false))
                                            .Scatter()
                                            .Call(r => sCDG.Get(r.Id))
                                            .Gather(cs => new QueryResult
                                            {
                                                Query = tempAq.Value(),
                                                Results = cs.ToList()
                                            })
                                        )
                        )
                       .Gather( // merge results from all alterations
                            rs => new QueryResult
                            {
                                RawQuery = query.Value(),
                                Results = rs.SelectMany(r => r.Results).ToList()
                            }
                       )
                );
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Csql.CreateService<IndexServe>("IndexServe");
        }
    }
}
