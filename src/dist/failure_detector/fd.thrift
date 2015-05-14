include "../../dsn.thrift"

namespace cpp dsn.fd

struct beacon_msg
{
    1: i64 time;
    2: dsn.end_point from;
    3: dsn.end_point to;
}

struct beacon_ack
{
    1: i64 time;
	2: dsn.end_point this_node;
	3: dsn.end_point primary_node;
    4: bool is_master;
    5: bool allowed;
}

service failure_detector
{
	beacon_ack ping(1:beacon_msg beacon)
}
