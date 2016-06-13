
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct Rank
{
    1: i32 Value = 0;
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

service DynamicRank
{
    Rank OnL2Rank(1:PerDocStaticRank pos);
}
