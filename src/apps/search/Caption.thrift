
namespace cpp dsn.app.search
namespace csharp dsn.app.search

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
    1: DocId DocIdentifier ;
    2: string Title = "";
    3: string CaptionHtml = "";
}

service CDG
{
    Caption Get(1:DocId id);
    ErrorResult Put(1:Caption caption);
}
