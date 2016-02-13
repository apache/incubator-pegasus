using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

using global::BondTransport;
using global::BondNetlibTransport;
using global::Microsoft.Bond;

using rDSN.Tron.Utility;

namespace rDSN.Tron.App
{
    public class SpellChecker_ServiceImpl : SpellChecker_Service
    {
        public override void Check(Request<global::rDSN.Tron.App.SpellCheckRequest, global::rDSN.Tron.App.SpellCheckResponse> call)
        {
            Console.Write(".");

            SpellCheckResponse r = new SpellCheckResponse();
            r.Candidates.AddLast(new SpellCheckResultEntry() { Score = 1.0, Word = call.RequestObject.Word });

            call.Dispatch(r);
        }
    }

    public class IFEX_ServiceImpl : IFEX_Service
    {
        public IFEX_ServiceImpl()
            : base()
        {
        }

        public override void OnSearchQuery(Request<global::rDSN.Tron.App.StringQuery, global::rDSN.Tron.App.StringQuery> call)
        {
            Console.Write(".");
            call.Dispatch(call.RequestObject);
        }
    }

    public class ALTA_ServiceImpl : ALTA_Service
    {
        public ALTA_ServiceImpl()
            : base()
        {
        }
    }

    public class IsCache_ServiceImpl : IsCache_Service
    {
        public IsCache_ServiceImpl()
            : base()
        {
        }

        public override void Get(Request<global::rDSN.Tron.App.StringQuery, global::rDSN.Tron.App.QueryResult> call)
        {
            Console.Write(".");

            call.Dispatch(new QueryResult()
            {
                RawQuery = call.RequestObject,
                Results = new List<Caption>()
            }
            );
        }

        public override void Put(Request<global::rDSN.Tron.App.QueryResult, global::rDSN.Tron.App.ErrorResult> call)
        {
            Console.Write(".");

            call.Dispatch(new ErrorResult());
        }
    }



    public class QU_ServiceImpl : QU_Service
    {
        public QU_ServiceImpl()
            : base()
        {
        }

        public override void OnQueryUnderstanding(Request<global::rDSN.Tron.App.StringQuery, global::rDSN.Tron.App.AlterativeQueryList> call)
        {
            Console.Write(".");

            var list = new AlterativeQueryList();
            var query = call.RequestObject;
            list.RawQuery = query;
            list.Alterations = new LinkedList<StringQuery>();

            int alterCount = (int) RandomGenerator.Random64(1, 100);

            for (int i = 0; i < alterCount; i++)
            {
                StringQuery q = new StringQuery();
                q.Query = query.Query + ".alter" + i;
                list.Alterations.AddLast(q);
            }

            call.Dispatch(list);
        }
    }



    public class QU2_ServiceImpl : QU2_Service
    {
        public QU2_ServiceImpl()
            : base()
        {
        }

        public override void OnQueryAnnotation(Request<global::rDSN.Tron.App.StringQuery, global::rDSN.Tron.App.AugmentedQuery> call)
        {
            Console.Write(".");

            StringQuery aq = new StringQuery();
            aq.Query = call.RequestObject.Query + ".annotated";

            var r = new AugmentedQuery();
            r.AlteredQuery = aq;
            r.RawQuery = call.RequestObject;
            r.QueryId = 0;
            r.TopX = 100;

            call.Dispatch(r);
        }
    }



    public class TLA_ServiceImpl : TLA_Service
    {
        public TLA_ServiceImpl()
            : base()
        {
        }
    }
    
    public class WebCache_ServiceImpl : WebCache_Service
    {
        public WebCache_ServiceImpl()
            : base()
        {
        }

        public override void Get(Request<global::rDSN.Tron.App.AugmentedQuery, global::rDSN.Tron.App.QueryResult> call)
        {
            Console.Write(".");

            call.Dispatch(
                new QueryResult() { Query = call.RequestObject, Results = new List<Caption>() }
                );
        }

        public override void Put(Request<global::rDSN.Tron.App.QueryResult, global::rDSN.Tron.App.ErrorResult> call)
        {
            Console.Write(".");

            call.Dispatch(
                new ErrorResult()
                );
        }
    }


    public class SaaS_ServiceImpl : SaaS_Service
    {
        public SaaS_ServiceImpl()
            : base()
        {
        }

        public override void OnL1Selection(Request<global::rDSN.Tron.App.AugmentedQuery, global::rDSN.Tron.App.StaticRankResult> call)
        {
            Console.Write(".");

            var r = new StaticRankResult();
            r.Query = call.RequestObject;
            r.Results = new LinkedList<PerDocStaticRank>();
            r.Results.AddLast(new PerDocStaticRank() { Pos = new DocPosition() { Position = 1, DocIdentity = new DocId() { URL = @"http://url1" } }, StaticRank = 10 });
            r.Results.AddLast(new PerDocStaticRank() { Pos = new DocPosition() { Position = 2, DocIdentity = new DocId() { URL = @"http://url2" } }, StaticRank = 7 });

            call.Dispatch(r);
        }
    }

    public class RaaS_ServiceImpl : RaaS_Service
    {
        public RaaS_ServiceImpl()
            : base()
        {
        }

        public override void OnL2Rank(Request<global::rDSN.Tron.App.PerDocStaticRank, global::rDSN.Tron.App.Rank> call)
        {
            Console.Write(".");

            Rank r = new Rank();
            r.Value = (new Random()).Next();
            call.Dispatch(r);
        }
    }

    public class CDG_ServiceImpl : CDG_Service
    {
        public CDG_ServiceImpl()
            : base()
        {
        }

        public override void Get(Request<global::rDSN.Tron.App.DocId, global::rDSN.Tron.App.Caption> call)
        {
            Console.Write(".");

            call.Dispatch(
                new Caption() { Title = "TestTitle", DocIdentifier = call.RequestObject }
                );
        }

        public override void Put(Request<global::rDSN.Tron.App.Caption, global::rDSN.Tron.App.ErrorResult> call)
        {
            Console.Write(".");

            call.Dispatch(
                new ErrorResult()
                );
        }
    }
}
