
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct StringQuery
{
    1: string Query;
}

struct AugmentedQuery
{
    1: i32 QueryId;
    2: StringQuery RawQuery;
    3: StringQuery AlteredQuery;
    4: i32 TopX;
}

service QU2
{
    AugmentedQuery OnQueryAnnotation(1:StringQuery query);
}
