
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct StringQuery
{
    1: string Query = "";
}

struct AugmentedQuery
{
    1: i32 QueryId = 0;
    2: StringQuery RawQuery;
    3: StringQuery AlteredQuery;
    4: i32 TopX;
}

struct DocId
{
    1: string URL = "";
}

struct DocPosition
{
    1: DocId DocIdentity;
    2: i32 Position = 0;
}

struct PerDocStaticRank
{
    1: DocPosition Pos;
    2: i32 StaticRank = 0;
}

struct StaticRankResult
{
    1: StringQuery Query;
    2: list<PerDocStaticRank> Results = [];
}

service StaticRank
{
    StaticRankResult OnL1Selection(1:StringQuery query);
}
