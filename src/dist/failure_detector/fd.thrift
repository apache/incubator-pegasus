include "../../dsn.thrift"

namespace cpp dsn.fd

struct beacon_msg
{
    1: i64 time;
    2: dsn.rpc_address from_addr;
    3: dsn.rpc_address to_addr;
}

struct beacon_ack
{
    1: i64 time;
    2: dsn.rpc_address this_node;
    3: dsn.rpc_address primary_node;
    4: bool is_master;
    5: bool allowed;
}

struct config_master_message
{
    1: dsn.rpc_address master;
    2: bool is_register;
}

service failure_detector
{
    beacon_ack ping(1:beacon_msg beacon)
}
