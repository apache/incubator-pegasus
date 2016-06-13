
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct StringQuery
{
    1: string Query = "";
}

struct AlterativeQueryList
{
    1: StringQuery RawQuery;
    2: list<StringQuery> Alterations = [];
}

service QU
{
    AlterativeQueryList OnQueryUnderstanding(1:StringQuery query);
}
