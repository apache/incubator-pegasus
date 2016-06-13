
namespace cpp dsn.app.search
namespace csharp dsn.app.search

struct SpellCheckRequest
{
    1: string Word = "";
}

struct SpellCheckResultEntry
{
    1: string Word = "";
    2: double Score = 0;
}

struct SpellCheckResponse
{
    1: list<SpellCheckResultEntry> Candidates = [];
}

service SpellChecker
{
    SpellCheckResponse Check(1:SpellCheckRequest word);
}

