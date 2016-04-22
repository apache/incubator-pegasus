
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct StringQuery
{
    1: string Query = "";
}

struct ErrorResult
{
    1: i32 ErrorCode = 0;
}

struct DocId
{
    1: string URL = "";
}

struct Caption
{
    1: DocId DocIdentifier;
    2: string Title = "";
    3: string CaptionHtml = "";
}

struct QueryResult
{
    1: StringQuery Query;
    2: list<Caption> Results = [];
}

service WebCache
{
    QueryResult Get(1:StringQuery query);
    ErrorResult Put(1:QueryResult result);
}
