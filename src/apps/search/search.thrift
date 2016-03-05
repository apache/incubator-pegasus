
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct StringQuery
{
    1: string Query;
}

struct ErrorResult
{
    1: i32 ErrorCode;
}

struct Rank
{
    1: i32 Value;
}

struct AlterativeQueryList
{
    1: StringQuery RawQuery;
    2: list<StringQuery> Alterations;
}

struct AugmentedQuery
{
    1: i32 QueryId;
    2: StringQuery RawQuery;
    3: StringQuery AlteredQuery;
    4: i32 TopX;
}

struct DocId
{
    1: string URL;
}

struct DocPosition
{
    1: DocId DocIdentity;
    2: i32 Position;
}

struct PerDocStaticRank
{
    1: DocPosition Pos;
    2: i32 StaticRank;
}

struct StaticRankResult
{
    1: AugmentedQuery Query;
    2: list<PerDocStaticRank> Results;
}

struct PerDocRank
{
    1: DocId Id;
    2: Rank  Rank;
}
        
struct Caption
{
    1: DocId DocIdentifier;
    2: string Title;
    3: string CaptionHtml;
}

struct QueryResult
{
    1: StringQuery RawQuery;
    2: AugmentedQuery Query;
    3: list<Caption> Results;
}

service IFEX
{
    StringQuery OnSearchQuery(1:StringQuery query);
}

service ALTA
{ 
}

service IsCache
{
    QueryResult Get(1:StringQuery query);
    ErrorResult Put(1:QueryResult result);
}

service QU
{
    AlterativeQueryList OnQueryUnderstanding(1:StringQuery query);
}

service QU2
{
    AugmentedQuery OnQueryAnnotation(1:StringQuery query);
}

service TLA
{ 
}

service WebCache
{
    QueryResult Get(1:AugmentedQuery query);
    ErrorResult Put(1:QueryResult result);
}

service SaaS
{
    StaticRankResult OnL1Selection(1:AugmentedQuery query);
}

service RaaS
{
    Rank OnL2Rank(1:PerDocStaticRank pos);
}

service CDG
{
    Caption Get(1:DocId id);
    ErrorResult Put(1:Caption caption);
}
