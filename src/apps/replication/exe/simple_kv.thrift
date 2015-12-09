namespace cpp dsn.replication.application

struct kv_pair
{
    1:string key;
    2:string value;
}

service simple_kv
{
    string read(1:string key);
    i32    write(2:kv_pair pr);
    i32    append(2:kv_pair pr);
}
