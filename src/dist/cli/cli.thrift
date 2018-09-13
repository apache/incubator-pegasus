// TODO: move this to dsn.service namespace
namespace cpp dsn

struct command
{
    1:string       cmd;
    2:list<string> arguments;
}

service cli
{
    string call(1:command c);
}
