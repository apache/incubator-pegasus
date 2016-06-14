namespace cpp dsn

struct error_code
{
    1: string code;
}

struct task_code
{
    1: string code;
}

struct gpid
{
    1: i64 id;
}

struct rpc_address
{
    1: string host;
    2: i32 port;
}